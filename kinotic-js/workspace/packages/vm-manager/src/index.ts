import { getJsBoxlite } from '@boxlite-ai/boxlite'
import { Kinotic } from '@kinotic-ai/core'
import { ensureNodeWebSocket } from '@kinotic-ai/core/node'
import { VmNodeRegistration } from '@kinotic-ai/system-api'
import { VmNodeOrchestrationServiceProxy } from '@kinotic-ai/system-api'
import { DefaultVmManager } from '@/internal/api/DefaultVmManager'
import { BoxliteProvider } from '@/internal/api/providers/BoxliteProvider'
import { CloudHypervisorProvider } from '@/internal/api/providers/CloudHypervisorProvider'
import { EgressPolicyManager } from '@/internal/api/network/EgressPolicyManager'
import type { IVmProvider } from '@/internal/api/providers/IVmProvider'
import { VmManagerConfig } from '@/api/VmManagerConfig'
import { AlloyManager } from '@/internal/api/telemetry/AlloyManager'
import { SYSTEM_API_ZONE, VmProviderType } from '@kinotic-ai/system-api'
import type { Workload } from '@kinotic-ai/management-api'
import type { WorkloadStatusReport } from '@kinotic-ai/system-api'
import Docker from 'dockerode'
import os from 'node:os'
import { join } from 'node:path'

const config = new VmManagerConfig()

const nodeId = config.nodeId ?? Bun.argv[2]
if (!nodeId) {
    console.error('Error: KINOTIC_NODE_ID environment variable or command line argument is required')
    process.exit(1)
}

const alloyManager = config.lokiUrl || config.tempoUrl || config.mimirUrl
    ? new AlloyManager({
        lokiUrl: config.lokiUrl || null,
        tempoUrl: config.tempoUrl || null,
        mimirUrl: config.mimirUrl || null,
        nodeId,
        dataDir: config.alloyDataDir,
    })
    : null
if (!config.lokiUrl) {
    console.warn('KINOTIC_LOKI_URL is not set — workload log shipping is disabled')
}
if (!config.tempoUrl) {
    console.warn('KINOTIC_TEMPO_URL is not set — workload trace shipping is disabled')
}
if (!config.mimirUrl) {
    console.warn('KINOTIC_MIMIR_URL is not set — workload metric shipping is disabled')
}

let heartbeatTimer: Timer | null = null
let shuttingDown = false
let reconnecting = false

// The reconnect delay doubles from the initial value up to the maximum. The jitter is added
// after that clamp, so a fleet of nodes that lost the server together stays spread out even
// once every one of them has settled at the maximum delay.
const RECONNECT_INITIAL_DELAY_MS = 2000
const RECONNECT_MAX_DELAY_MS = 120000
const RECONNECT_MIN_JITTER_MS = 1000
const RECONNECT_MAX_JITTER_MS = 5000

// How long a graceful disconnect may take before the process exits with the socket still open. A
// half-open socket answers neither the DISCONNECT frame nor the close, so a graceful disconnect on
// one lasts the heartbeat timeout plus the WebSocket close timeout; the exit closes it instead.
const DISCONNECT_TIMEOUT_MS = 5000

// The node runs whichever provider it is configured for and nothing else, so a provider it
// cannot construct is a fatal misconfiguration rather than a capability to omit
function createProvider(reportStatus: (workload: Workload) => void): IVmProvider {
    let ret: IVmProvider
    if (config.providerType === VmProviderType.BOXLITE) {
        // boxlite's runtime is a process-wide singleton that resolves its own home; pointing
        // it at the configured one makes the store it writes boxes to and the directory the
        // provider reads them from a single value, without a node having to set BOXLITE_HOME.
        // One-shot, so it must run before anything asks for the runtime.
        getJsBoxlite().initDefault({ homeDir: config.boxliteHome })
        ret = new BoxliteProvider(config.boxliteHome, config.vmLogsDir, config.vmStateDir,
                                  config.workloadDataDir, reportStatus, alloyManager)
    } else if (config.providerType === VmProviderType.CLOUD_HYPERVISOR) {
        const egress = new EgressPolicyManager(config.workloadDns ?? null)
        if (!egress.enforces()) {
            console.warn('This node does not deny workload egress by default — a workload can reach '
                         + 'anything its address can route to. See deployment/vm-node/README.md')
        }
        ret = new CloudHypervisorProvider(join(config.vmStateDir, 'cloud-hypervisor'),
                                          new Docker(),
                                          config.workloadDataDir,
                                          egress,
                                          config.workloadDns ?? null,
                                          reportStatus,
                                          alloyManager)
    } else {
        throw new Error(`No provider implementation for ${config.providerType}`)
    }
    return ret
}

function toStatusReport(workload: Workload): WorkloadStatusReport {
    return {
        workloadId: workload.id!,
        status: workload.status,
        exitCode: workload.exitCode ?? null,
        updated: workload.updated ?? Date.now(),
    }
}

/**
 * Why this node cannot ship the telemetry of the workloads it runs, if it cannot. A workload
 * whose output goes nowhere is not one this node should be given, so this joins the invariants
 * the provider checks — the node keeps what it is running and stops being offered more.
 *
 * Only a node that was asked to ship can fail to: one started without KINOTIC_LOKI_URL,
 * KINOTIC_TEMPO_URL, and KINOTIC_MIMIR_URL has already said so at startup.
 */
function shippingProblems(): string[] {
    const problem = alloyManager?.shippingProblem() ?? null
    return problem !== null ? [problem] : []
}

/**
 * Rejoins the server after the event bus fails fatally. An ordinary dropped connection is the
 * STOMP client's own reconnect; a fatal error — a server ERROR frame, credentials that no longer
 * resolve — deactivates the connection for good instead, so without this the node keeps running
 * its workloads while the orchestrator sees it go offline. Retries until it connects: the
 * workloads outlive the server being down.
 */
function reconnectOnFatalError() {
    Kinotic.eventBus.fatalErrors.subscribe(async (error: Error) => {
        // Each failed reconnect signals another fatal error, which lands back here
        if (reconnecting || shuttingDown) {
            return
        }
        reconnecting = true
        console.error('Kinotic connection failed fatally, reconnecting:', error)
        try {
            let delayMs = RECONNECT_INITIAL_DELAY_MS
            while (!shuttingDown && !Kinotic.eventBus.isConnectionActive()) {
                const jitterMs = RECONNECT_MIN_JITTER_MS
                                 + Math.random() * (RECONNECT_MAX_JITTER_MS - RECONNECT_MIN_JITTER_MS)
                await new Promise(resolve => setTimeout(resolve, delayMs + jitterMs))
                try {
                    // Published services and observed destinations re-subscribe with the new
                    // connection, and the next heartbeat marks the node online again
                    await Kinotic.connect()
                    console.log('Reconnected to the Kinotic server')
                } catch (e) {
                    delayMs = Math.min(delayMs * 2, RECONNECT_MAX_DELAY_MS)
                    console.error(`Reconnect failed, retrying in ~${delayMs / 1000}s:`, e)
                }
            }
        } finally {
            reconnecting = false
        }
    })
}

function startHeartbeat(nodeOrchestrator: VmNodeOrchestrationServiceProxy,
                        vmManager: DefaultVmManager,
                        provider: IVmProvider) {
    // The next beat is scheduled once this one has finished, so a heartbeat that outlives its
    // interval while the server is slow or unreachable never overlaps the one after it
    const beat = async () => {
        try {
            // A node that stopped enforcing something keeps its workloads but takes no more
            await nodeOrchestrator.heartbeat(nodeId!, [...await provider.checkNodeHealth(),
                                                      ...shippingProblems()])
            // Snapshot reconciliation: re-reporting everything converges any transition
            // whose push was lost while the server was unreachable
            const workloads = await vmManager.listWorkloads()
            if (workloads.length > 0) {
                await nodeOrchestrator.reportWorkloadStatus(nodeId!, workloads.map(toStatusReport))
            }
        } catch (error) {
            console.error('Heartbeat failed:', error)
        } finally {
            if (!shuttingDown) {
                heartbeatTimer = setTimeout(beat, config.heartbeatIntervalMs)
            }
        }
    }
    heartbeatTimer = setTimeout(beat, config.heartbeatIntervalMs)
}

async function start() {
    // The vm-manager runs as a system participant, so its VmManager service registers in the
    // system zone. Must be set before DefaultVmManager is instantiated (@Publish registers there).
    Kinotic.zonePrefix = SYSTEM_API_ZONE

    // Server and credentials resolve from the environment: KINOTIC_SERVER_HOST/PORT/USE_SSL
    // and KINOTIC_CLIENT_ID/KINOTIC_CLIENT_SECRET (or KINOTIC_TOKEN).
    ensureNodeWebSocket()
    await Kinotic.connect()
    const server = Kinotic.eventBus.serverInfo
    console.log(`Connected to Kinotic server at ${server?.host}:${server?.port}`)

    // Installed after the first connect so a fatal error there still fails startup
    reconnectOnFatalError()

    const nodeOrchestrator = new VmNodeOrchestrationServiceProxy(Kinotic)

    // Reattach to workloads a previous vm-manager process left running before the
    // VmManager service is published and can receive new workload operations. Every
    // node-side status transition is pushed so the server tracks the workload's real
    // state; a failed push is only logged — the heartbeat snapshot reconciles it.
    const reportStatus = (workload: Workload) => {
        nodeOrchestrator.reportWorkloadStatus(nodeId!, [toStatusReport(workload)])
                        .catch(error => console.error('Failed to report workload status:', error))
    }
    const provider = createProvider(reportStatus)
    await provider.recover()

    // Create and register the VmManager service (automatically registered via @Publish + @Scope)
    const vmManager = new DefaultVmManager(nodeId!, provider, alloyManager)

    // Resume shipping the recovered workloads' telemetry, which also downloads and launches
    // Alloy here rather than inside whichever startWorkload call arrives first
    await vmManager.refreshShipping()

    // Build registration info from system resources
    const registration = new VmNodeRegistration(nodeId!, os.hostname(), os.hostname())
    registration.providerType = provider.type
    registration.totalCpus = os.cpus().length
    registration.totalMemoryMb = Math.floor(os.totalmem() / (1024 * 1024))
    registration.totalDiskMb = await provider.totalDiskMb()
    registration.workloadDataDir = config.workloadDataDir

    // Register this node with the VmNodeOrchestrationService on the server
    await nodeOrchestrator.registerNode(registration)

    console.log(`VM Manager registered on node: ${nodeId} (provider ${provider.type})`)
    console.log(`  CPUs: ${registration.totalCpus}, Memory: ${registration.totalMemoryMb}MB, `
                + `Disk: ${registration.totalDiskMb}MB`)

    // Start sending periodic heartbeats
    startHeartbeat(nodeOrchestrator, vmManager, provider)
    console.log(`Heartbeat started (every ${config.heartbeatIntervalMs / 1000}s)`)
}

// Graceful shutdown
async function shutdown() {
    console.log('Shutting down VM Manager...')
    shuttingDown = true
    if (heartbeatTimer) {
        clearTimeout(heartbeatTimer)
    }
    await alloyManager?.stop()
    await Promise.race([Kinotic.disconnect(), Bun.sleep(DISCONNECT_TIMEOUT_MS)])
    process.exit(0)
}

process.on('SIGINT', shutdown)
process.on('SIGTERM', shutdown)

start().catch(error => {
    console.error('Failed to start VM Manager:', error)
    process.exit(1)
})

export type { IVmManager } from '@/api/IVmManager'
export type { IVmProvider } from '@/internal/api/providers/IVmProvider'
export type { VolumeMount } from '@kinotic-ai/management-api'
export { VmManagerConfig } from '@/api/VmManagerConfig'
