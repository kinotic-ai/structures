

package org.kinotic.core.internal.api.service.json;


import lombok.Getter;
import org.kinotic.core.api.config.KinoticProperties;
import org.kinotic.core.api.event.Event;
import org.kinotic.core.api.event.EventConstants;
import org.kinotic.core.api.event.Metadata;
import org.kinotic.core.api.security.Participant;
import org.kinotic.core.internal.api.service.invoker.ServiceInvocationSupervisor;
import org.kinotic.core.api.security.SecurityContext;
import org.kinotic.core.internal.utils.EventUtil;
import org.apache.commons.lang3.Validate;
import org.springframework.core.GenericTypeResolver;
import org.springframework.core.MethodParameter;
import org.springframework.core.ReactiveAdapterRegistry;
import org.springframework.core.codec.CodecException;
import org.springframework.core.codec.DecodingException;
import org.springframework.core.codec.EncodingException;
import org.springframework.util.MimeTypeUtils;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JavaType;
import tools.jackson.databind.ObjectReader;
import tools.jackson.databind.exc.InvalidDefinitionException;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.type.TypeFactory;
import tools.jackson.databind.util.TokenBuffer;

import java.lang.reflect.Type;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 *
 * Created by Navid Mitchell on 2019-04-08.
 */
public abstract class AbstractJacksonSupport {

    @Getter
    private final JsonMapper jsonMapper;
    private final SecurityContext securityContext;
    private final ReactiveAdapterRegistry reactiveAdapterRegistry;
    private final KinoticProperties kinoticProperties;

    public AbstractJacksonSupport(JsonMapper jsonMapper,
                                  ReactiveAdapterRegistry reactiveAdapterRegistry,
                                  KinoticProperties kinoticProperties,
                                  SecurityContext securityContext) {
        this.jsonMapper = jsonMapper;
        this.securityContext = securityContext;
        this.reactiveAdapterRegistry = reactiveAdapterRegistry;
        this.kinoticProperties = kinoticProperties;

    }

    /**
     * Tests if the content is considered JSON
     * @param incomingMetadata to evaluate
     * @return true if the content-type header of the message is application/json
     */
    protected boolean containsJsonContent(Metadata incomingMetadata) {
        boolean ret = false;
        String contentType = incomingMetadata.get(EventConstants.CONTENT_TYPE_HEADER);
        if(contentType != null && !contentType.isEmpty()){
            ret =  MimeTypeUtils.APPLICATION_JSON_VALUE.contentEquals(contentType);
        }
        return ret;
    }

    /**
     * Resolves a {@link Participant} parameter from the current Vert.x context, narrowed to the
     * {@link Participant} subtype the parameter declares.
     *
     * @param methodParameter the {@link Participant} parameter being resolved
     * @return the participant bound to the current Vert.x context
     * @throws IllegalStateException if no participant is bound to the current Vert.x context
     * @throws org.kinotic.core.api.exceptions.AuthorizationException if the participant is not of
     *         the declared subtype
     */
    protected Participant resolveParticipant(MethodParameter methodParameter) {
        // A service declares the participant scope it serves, so narrowing here turns a caller the
        // service does not serve into an authorization failure rather than an "argument type
        // mismatch" out of Method.invoke
        @SuppressWarnings("unchecked")
        Class<? extends Participant> participantType =
                (Class<? extends Participant>) methodParameter.getParameterType();
        return securityContext.requireParticipant(participantType);
    }

    /**
     * Transforms the JSON content to Java objects using the given expected parameter types
     * @param event the message containing the JSON content to be converted
     * @param parameters to determine the correct type for the {@link TokenBuffer} being decoded.
     * @param dataInArray if true the incoming data is expected to be within an array such as when decoding input arguments
     *
     * @return the deserialized JSON as Java objects
     */
    protected Object[] createJavaObjectsFromJsonEvent(Event<byte[]> event, MethodParameter[] parameters, boolean dataInArray){
        Validate.notNull(event, "event must not be null");
        Validate.notNull(parameters, "parameters must not be null");

        List<TokenBuffer> tokens = JacksonTokenizer.tokenize(event.data(),
                                                             jsonMapper,
                                                             dataInArray,
                                                             kinoticProperties.getMaxEventPayloadSize());

        List<Object> ret = new LinkedList<>();
        int tokenCount = tokens.size();

        // Count the number of parameters that come from JSON tokens (i.e. not Participant)
        int jsonParamCount = 0;
        for (MethodParameter p : parameters) {
            if (!Participant.class.isAssignableFrom(p.nestedIfOptional().getParameterType())) {
                jsonParamCount++;
            }
        }

        if (tokenCount > jsonParamCount) {
            throw new IllegalArgumentException("Received too many json arguments, Expected: " + jsonParamCount + " Got: " + tokenCount);
        }

        int tokenIdx = 0;
        for (MethodParameter methodParameter : parameters) {

            methodParameter = methodParameter.nestedIfOptional();

            // If the parameter is a Participant we get this from the Vert.x context
            if (Participant.class.isAssignableFrom(methodParameter.getParameterType())) {

                ret.add(resolveParticipant(methodParameter));

            } else {
                if (tokenIdx >= tokenCount) {
                    throw new IllegalArgumentException("Received too few json arguments, Expected: " + jsonParamCount + " Got: " + tokenCount);
                }

                Object arg = decodeInternal(tokens.get(tokenIdx), methodParameter);
                ret.add(arg);
                tokenIdx++;
            }
        }
        return ret.toArray();
    }

    private Object decodeInternal(TokenBuffer tokenBuffer, MethodParameter methodParameter){
        // Unwrap async classes, this is also used for method return values so this handles that..
        if(reactiveAdapterRegistry.getAdapter(methodParameter.getParameterType()) != null){
            methodParameter = methodParameter.nested();
        }

        Object ret;

        // The parser will return null for void so we don't parse void
        if(!Void.class.isAssignableFrom(methodParameter.getParameterType())){

            // Support passing the TokenBuffer directly
            if (TokenBuffer.class.isAssignableFrom(methodParameter.getParameterType())) {

                ret = tokenBuffer;

            } else {

                JavaType javaType = getJavaType(methodParameter);
                ObjectReader reader = getJsonMapper().readerFor(javaType);

                try {

                    ret = reader.readValue(tokenBuffer.asParser(getJsonMapper()._deserializationContext()));

                } catch (InvalidDefinitionException ex) {
                    throw new CodecException("Type definition error: " + ex.getType(), ex);
                } catch (JacksonException ex) {
                    throw new DecodingException("JSON decoding error: " + ex.getOriginalMessage(), ex);
                }
            }
        }else{
            ret = Void.TYPE;
        }

        return ret;
    }

    protected JavaType getJavaType(MethodParameter methodParameter){
        Type targetType = methodParameter.getNestedGenericParameterType();
        Class<?> contextClass = methodParameter.getContainingClass();
        TypeFactory typeFactory = this.jsonMapper.getTypeFactory();
        return typeFactory.constructType(GenericTypeResolver.resolveType(targetType, contextClass));
    }

    /**
     * Creates a {@link Event} that can be sent based on the incomingMessage headers and the data to use as the body
     * @param incomingMetadata the original {@link Metadata} sent to the {@link ServiceInvocationSupervisor}
     * @param headers key value pairs that will be added to the outgoing headers
     * @param body the value that will be converted to a JSON string and set as the body
     * @return the {@link Event} to send
     */
    protected Event<byte[]> createOutgoingEvent(Metadata incomingMetadata, Map<String, String> headers, Object body){
        return EventUtil.createReplyEvent(incomingMetadata, headers, () -> {
            byte[] jsonBytes;
            try {

                jsonBytes = jsonMapper.writeValueAsBytes(body);

            } catch (JacksonException e) {
                throw new EncodingException("JSON encoding error: " + e.getOriginalMessage(), e);
            }
            return jsonBytes;
        });
    }


}
