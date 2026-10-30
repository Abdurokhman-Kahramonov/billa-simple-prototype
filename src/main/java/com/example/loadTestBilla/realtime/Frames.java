package com.example.loadTestBilla.realtime;

import java.util.LinkedHashMap;
import java.util.Map;


public final class Frames {

	private Frames() {
	}

	public static Map<String, Object> of(String type, Object... keyValues) {
		Map<String, Object> frame = new LinkedHashMap<>();
		frame.put("type", type);
		for (int i = 0; i + 1 < keyValues.length; i += 2) {
			if (keyValues[i + 1] != null) {
				frame.put((String) keyValues[i], keyValues[i + 1]);
			}
		}
		return frame;
	}

}
