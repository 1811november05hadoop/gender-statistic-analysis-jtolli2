
public class RegexTest {

	public static void main(String[] args) {
		String line = "\"France\",\"FRA\",\"Gross graduation ratio, tertiary, female (%)\",\"SE.TER.CMPL.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"48.72098\",\"43.48583\",\"\",\"47.27924\",\"49.29758\",\"38.57893\",\"39.91633\",\"40.28646\",\"40.28185\",\"39.72192\",\"41.0811\",\"\",\"\",\"45.42602\",\"49.86369\",\"51.55747\",\"\",\"\",";

		if(line.contains("SE.TER.CMPL.FE")) {
			String[] entry = line.split(",");
			float avg = 0;
			float count = 1;
			for (String string : entry) {
				String cleanString = string.substring(1, string.length()-1);
				
				if(cleanString.matches("^\\d*\\.\\d+|\\d+\\.\\d*$")) {
					System.out.println(cleanString);
					avg += Float.parseFloat(cleanString);
					count++;
				}
			}
			avg /= count;
			System.out.println(entry[0].substring(1, entry[0].length()-1));
			System.out.println(avg);
		}

	}
}