import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CSVReader2 {
    public static List<Object[]> reader() throws IOException {
        int count = 0;
        String file = "D:/Coursework/Query Processing/CalciteProject/src/main/resources/data/drama_genres.csv";
        List<Object[]> content = new ArrayList<>();
        try(BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line = "";
            while ((line = br.readLine()) != null) {
                content.add(line.split(","));
            }
        } catch (FileNotFoundException e) {
            //Some error logging
        }
        return content;
    }
}
