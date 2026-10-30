package com.example.loadTestBilla.common;

public final class Trace {

	public enum Area {

		HTTP("\u001B[36m"), // cyan
		SOCKET("\u001B[35m"), // magenta
		MATCHMAKING("\u001B[33m"), // yellow
		MATCH("\u001B[32m"), // green
		REDIS("\u001B[91m"), // bright red
		DB("\u001B[94m"); // bright blue

		private final String color;

		Area(String color) {
			this.color = color;
		}

	}

	private static final String RESET = "\u001B[0m";

	private static final boolean ON = !"false".equalsIgnoreCase(System.getProperty("trace"));

	private Trace() {
	}

	public static void call(Area area, String classAndMethod) {
		if (ON) {
			System.out.println(area.color + String.format("%-14s", "[" + area + "]") + classAndMethod + RESET);
		}
	}


	public static void call(Area area, String classAndMethod, Object detail) {
		if (ON) {
			System.out.println(
					area.color + String.format("%-14s", "[" + area + "]") + classAndMethod + " -> " + detail + RESET);
		}
	}

}
