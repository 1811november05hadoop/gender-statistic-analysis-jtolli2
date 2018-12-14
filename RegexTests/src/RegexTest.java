
public class RegexTest {

	public static void main(String[] args) {
		String line = "\"Bahrain\",\"BHR\",\"Gross graduation ratio, tertiary, female (%)\",\"SE.TER.CMPL.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"1.0\",\"\",\"1.0\",\"1.0\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",";

		if(line.contains("SE.TER.CMPL.FE")) {
			String[] entry = line.split(",");
			float avg = 0;
			float count = 0;
			float previous = 0;
			float current = 0;
			for (int i = entry.length-1-16; i < entry.length; i++) {
				String cleanString = entry[i].substring(1, entry[i].length()-1);
				//Only digits with decimals
				if(cleanString.matches("^\\d*\\.\\d+|\\d+\\.\\d*$")) {
					if(count > 0) {
						current = Float.parseFloat(cleanString);
						avg += (current - previous);
						System.out.println(avg);
						previous = current;
					}
					else {
						current = Float.parseFloat(cleanString);
						System.out.println(avg);
						previous = current;
					}
					count++;
				}
			}
			System.out.println(avg);
			System.out.println(count);
			avg /= count;
			System.out.println(entry[0].substring(1, entry[0].length()-1));
			System.out.println(avg);
		}

	}
}