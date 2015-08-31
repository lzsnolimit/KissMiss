package kimiss;

public class Watch {
	private static long begin = 0;
	private static long end = 0;
	private static long duration=0;

	public static long start() {
		begin = System.currentTimeMillis();
		end=begin;
		duration=0;
		return begin;
	}

	public static long stop() {
		duration +=System.currentTimeMillis()-end;
		end = System.currentTimeMillis();
		return end;
	}

	public static long getTime() {
		return duration;
	}
}
