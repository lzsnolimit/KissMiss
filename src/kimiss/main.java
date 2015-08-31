package kimiss;



public class main {
	
	public static void startThreads(int size) {
		kimissRequest[] threads = new kimissRequest[size];
		for (int i = 0; i < size; i++) {
			threads[i] = new kimissRequest();
			threads[i].start();
		}
		for (int i = 0; i < size; i++) {
			try {
				threads[i].join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		//DomainNames.replaceDomains();
	}
	public static void main(String[] args) throws Exception {
		Hbase.setStrings("kismissProduct", "com");
		//Hbase.createTable();
		Watch.start();
		startThreads(5);
		Watch.stop();
		System.out.println(Watch.getTime()/1000+" seconds");
		kimissRequest.writeErrors();
		kimissRequest.read();
	}
}
