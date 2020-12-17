package mapreduce;

public class Test {

	public static void main(String[] args) {
		String str = "02_11_2012_12_32_10 132.227.045.028 musique+orientale";
		
//		StringTokenizer stoken = new StringTokenizer(str);
//		while(stoken.hasMoreTokens()) {
//			System.out.println(stoken.nextToken());
//		}
		
		String[] s = str.split(" ");
		String[] splited = s[0].split("_");
		String[] mots = s[2].split("\\+");
		
		if (Integer.parseInt(splited[4]) < 30) {
			System.out.println("entre "+splited[3]+"h00 et "+ splited[3]+"h29");
		}else {
			System.out.println("entre "+splited[3]+"h29 et "+ (Integer.parseInt(splited[3])+1)+"h00");
		}
		
		for (String m : mots) {
			System.out.println(m);
		}
		
		System.out.println(Integer.parseInt(splited[0]));
		
		
	}

}
