/*
 * Copyright 2002-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kinotic.core.internal.api.service.json;

import org.springframework.core.codec.DecodingException;
import org.springframework.core.io.buffer.DataBufferLimitException;
import tools.jackson.core.JacksonException;
import tools.jackson.core.JsonParser;
import tools.jackson.core.JsonToken;
import tools.jackson.core.async.ByteArrayFeeder;
import tools.jackson.core.async.ByteBufferFeeder;
import tools.jackson.core.async.NonBlockingInputFeeder;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.util.TokenBuffer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * NOTE: Copied directly from Spring Framework Web, since this is package private there.
 *       And modified to support nested arrays when tokenizeArrayElements is true. Line 159.
 *       And since we have a copy we make small modifications to improve performance for our use case.
 * Transforms a JSON stream of arbitrary size, byte array
 * chunks into a {@code Flux<TokenBuffer>} where each token buffer is a
 * well-formed JSON object with Jackson 3.x.
 *
 * @author Sebastien Deleuze
 * @since 7.0
 */
final class JacksonTokenizer {

	private final JsonParser parser;

	private final NonBlockingInputFeeder inputFeeder;

	private final boolean tokenizeArrayElements;

	private final int maxInMemorySize;

	private int objectDepth;

	private int arrayDepth;

	private int byteCount;

	private TokenBuffer tokenBuffer;


	private JacksonTokenizer(JsonParser parser, boolean tokenizeArrayElements, int maxInMemorySize) {
		this.parser = parser;
		this.inputFeeder = this.parser.nonBlockingInputFeeder();
		this.tokenizeArrayElements = tokenizeArrayElements;
		this.maxInMemorySize = maxInMemorySize;
		this.tokenBuffer = createToken();
	}


	private List<TokenBuffer> tokenize(byte[] bytes) {
		try {
			List<TokenBuffer> tokens = new ArrayList<>();
			if (this.inputFeeder instanceof ByteBufferFeeder byteBufferFeeder) {
				byteBufferFeeder.feedInput(ByteBuffer.wrap(bytes));
				parseTokens(tokens);
			}
			else if (this.inputFeeder instanceof ByteArrayFeeder byteArrayFeeder) {
				byteArrayFeeder.feedInput(bytes, 0, bytes.length);
				parseTokens(tokens);
			}
			assertInMemorySize(bytes.length, tokens);
			return tokens;
		}
		catch (JacksonException ex) {
			throw new DecodingException("JSON decoding error: " + ex.getOriginalMessage(), ex);
		}
	}

	private List<TokenBuffer> endOfInput() {
		this.inputFeeder.endOfInput();
		try {
			List<TokenBuffer> tokens = new ArrayList<>();
			parseTokens(tokens);
			return tokens;
		}
		catch (JacksonException ex) {
			throw new DecodingException("JSON decoding error: " + ex.getOriginalMessage(), ex);
		}
	}

	private void parseTokens(List<TokenBuffer> tokens) {
		// SPR-16151: Smile data format uses null to separate documents
		boolean previousNull = false;
		while (!this.parser.isClosed()) {
			JsonToken token = this.parser.nextToken();
			if (token == JsonToken.NOT_AVAILABLE ||
					token == null && previousNull) {
				break;
			}
			else if (token == null ) { // !previousNull
				previousNull = true;
				continue;
			}
			else {
				previousNull = false;
			}
			updateDepth(token);
			if (!this.tokenizeArrayElements) {
				processTokenNormal(token, tokens);
			}
			else {
				processTokenArray(token, tokens);
			}
		}
	}

	private void updateDepth(JsonToken token) {
		switch (token) {
			case START_OBJECT -> this.objectDepth++;
			case END_OBJECT -> this.objectDepth--;
			case START_ARRAY -> this.arrayDepth++;
			case END_ARRAY -> this.arrayDepth--;
		}
	}

	private void processTokenNormal(JsonToken token, List<TokenBuffer> result) {
		this.tokenBuffer.copyCurrentEvent(this.parser);

		if ((token.isStructEnd() || token.isScalarValue()) && this.objectDepth == 0 && this.arrayDepth == 0) {
			result.add(this.tokenBuffer);
			this.tokenBuffer = createToken();
		}
	}

	private void processTokenArray(JsonToken token, List<TokenBuffer> result) {
		if (!isTopLevelArrayToken(token)) {
			this.tokenBuffer.copyCurrentEvent(this.parser);
		}

		// Modified this line so Tokenizer will handle nested arrays properly
		if (this.objectDepth == 0 && (this.arrayDepth == 0 || this.arrayDepth == 1)
				&& (token == JsonToken.END_OBJECT || token.isScalarValue() || (token == JsonToken.END_ARRAY && this.arrayDepth == 1))) {
			result.add(this.tokenBuffer);
			this.tokenBuffer = createToken();
		}
	}

	private TokenBuffer createToken() {
        return TokenBuffer.forBuffering(this.parser, this.parser.objectReadContext());
	}


	private boolean isTopLevelArrayToken(JsonToken token) {
		return this.objectDepth == 0 && ((token == JsonToken.START_ARRAY && this.arrayDepth == 1) ||
				(token == JsonToken.END_ARRAY && this.arrayDepth == 0));
	}

	private void assertInMemorySize(int currentBufferSize, List<TokenBuffer> result) {
		if (this.maxInMemorySize >= 0) {
			if (!result.isEmpty()) {
				this.byteCount = 0;
			}
			else if (currentBufferSize > Integer.MAX_VALUE - this.byteCount) {
				raiseLimitException();
			}
			else {
				this.byteCount += currentBufferSize;
				if (this.byteCount > this.maxInMemorySize) {
					raiseLimitException();
				}
			}
		}
	}

	private void raiseLimitException() {
		throw new DataBufferLimitException(
				"Exceeded limit on max bytes per JSON object: " + this.maxInMemorySize);
	}


	/**
	 * Tokenize the given JSON bytes into a list of {@code TokenBuffer}s, one per top-level value.
	 * @param bytes the complete JSON document
	 * @param objectMapper the current mapper instance
	 * @param tokenizeArrays if {@code true} and the "top level" JSON object is
	 * an array, each element is returned individually
	 * @param maxInMemorySize maximum memory size
	 * @return the resulting token buffers
	 * @throws DecodingException when the bytes are not valid JSON
	 * @throws DataBufferLimitException when a single value exceeds {@code maxInMemorySize}
	 */
	public static List<TokenBuffer> tokenize(byte[] bytes,
											 ObjectMapper objectMapper,
											 boolean tokenizeArrays,
											 int maxInMemorySize) {
		JsonParser parser;
		try {
			parser = objectMapper.createNonBlockingByteBufferParser();
		}
		catch (UnsupportedOperationException ex) {
			parser = objectMapper.createNonBlockingByteArrayParser();
		}
		JacksonTokenizer tokenizer = new JacksonTokenizer(parser, tokenizeArrays, maxInMemorySize);
		List<TokenBuffer> tokens = new ArrayList<>(tokenizer.tokenize(bytes));
		tokens.addAll(tokenizer.endOfInput());
		return tokens;
	}

}
