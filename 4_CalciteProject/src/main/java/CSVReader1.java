import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class CSVReader1 {
    public static List<List<Object[]>> reader() throws IOException {
        String file = "D:/Coursework/Query Processing/CalciteProject/src/main/resources/data/BOOK_DATA.csv";
        List<List<Object[]>> Files = new ArrayList<>();
        List<Object[]> content = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            try(BufferedReader br = new BufferedReader(new FileReader(file))) {
                String line = "";
                while ((line = br.readLine()) != null) {
                    content.add(line.split(","));
                    System.out.println(content);

                }
            } catch (FileNotFoundException e) {
                //Some error logging
            }
            Files.add(content);


        }

        return Files;
    }
}