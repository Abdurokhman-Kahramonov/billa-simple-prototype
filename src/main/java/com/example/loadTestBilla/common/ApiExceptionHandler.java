package com.example.loadTestBilla.common;

import java.util.Map;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class ApiExceptionHandler {

	@ExceptionHandler(ApiException.class)
	public ResponseEntity<Map<String, String>> handle(ApiException ex) {
		Trace.call(Trace.Area.HTTP, "ApiExceptionHandler.handle", ex.code());
		return ResponseEntity.status(ex.status()).body(Map.of("error", ex.code()));
	}

}
