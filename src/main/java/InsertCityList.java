import com.google.gson.Gson;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class InsertCityList {
    public static void main(String[] args) {
        // DB init
        SQLConnection.init();

        jsonFileToDB();
    }

    private class Coord {
        double lon, lat;
        Coord(double lon, double lat) {
            this.lon = lon;
            this.lat = lat;
        }
    }

    private class Geo {
        public int id;
        public String name, country;
        public Coord coord;

        public Geo(int id, String name, String country, Coord coord) {
            this.id = id;
            this.name = name;
            this.country = country;
            this.coord = coord;
        }
    }

    public static void jsonFileToDB() {
        String filePath = "C:\\Users\\Xylian\\IdeaProjects\\iWeather\\src\\maincity.list.json";
        try {
            String content = new String(Files.readAllBytes(Paths.get(filePath)));

            Gson gson = new Gson();

            Geo[] datasets = gson.fromJson(content, Geo[].class);

            for (Geo dataset : datasets) {
                String sql = "INSERT INTO `geo`(`own_ID`, `name`, `country`, `lon`, `lat`) "
                        + "VALUES ('"+ dataset.id +"','"+ dataset.name.replaceAll("'", "")
                        + "','"+ dataset.country +"','"+ dataset.coord.lon +"','"+ dataset.coord.lat +"');";
                System.out.println(sql);
                SQLConnection.update(sql);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
