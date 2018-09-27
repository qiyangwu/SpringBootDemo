package framework.schedulejob.job.ordinary;

import java.util.Calendar;
import java.util.TimeZone;

public class Test {
	public static byte[] stringToBytesUTFCustom(String str) {
		char[] buffer = str.toCharArray();
		byte[] b = new byte[buffer.length << 1];
		for (int i = 0; i < buffer.length; i++) {
			int bpos = i << 1;
			b[bpos] = (byte) ((buffer[i] & 0xFF00) >> 8);
			b[bpos + 1] = (byte) (buffer[i] & 0x00FF);
		}
		return b;
	}

	public static String bytesToStringUTFCustom(byte[] bytes) {
		char[] buffer = new char[bytes.length >> 1];
		for (int i = 0; i < buffer.length; i++) {
			int bpos = i << 1;
			char c = (char) (((bytes[bpos] & 0x00FF) << 8) + (bytes[bpos + 1] & 0x00FF));
			buffer[i] = c;
		}
		return new String(buffer);
	}

	public static void main(String[] args) throws Exception {
		Test laiYiGe = new Test();
		byte[] dd = new byte[10];
		dd[0] = 90;
		String pia = laiYiGe.bytesToStringUTFCustom(dd);
		System.out.println(pia);
	}
}
