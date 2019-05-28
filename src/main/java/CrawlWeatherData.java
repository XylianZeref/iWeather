import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.io.*;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

public class CrawlWeatherData {
    public static final String API_KEY_OPEN_WEATHER_MAP = "0748fd70f9msh3ed6f2f69059c2ap196176jsndaf369ea7081";

    public static void main(String[] args) {
        jsonFileToDB();

        // DB
        SQLConnection.init();
        ResultSet a = SQLConnection.query("SHOW DATABASES");
        SQLConnection.close();
        try {
            while(a.next()) {
                System.out.println(a.getString("Database"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        String geo_city = "Hanover";
        String geo_lat = "";
        String geo_lon = "";

        // crawl weather data: Open Weather Map
        String[][] headers = { {"X-RapidAPI-Host", "community-open-weather-map.p.rapidapi.com"},
                    {"X-RapidAPI-Key", API_KEY_OPEN_WEATHER_MAP} };

        String response = executeHttpRequest("https://community-open-weather-map.p.rapidapi.com/weather",
                "&lat="+geo_lat
                        + "&lon="+geo_lon
                        + "&units=metric"
                        + "&mode=json"
                        + "&q="+geo_city,
                "GET", headers);
        System.out.println(response);

        SQLConnection.shutdown();
    }

    public static String executeHttpRequest(String targetURL, String urlParameters, String method, String[][] headers) {
        if (method.equals("GET")) {
            targetURL += "?" + urlParameters;
        }

        HttpURLConnection connection = null;

        try {
            URL url = new URL(targetURL);
            connection = (HttpURLConnection) url.openConnection();

            if(headers.length > 0) {
                for (int i = 0; i < headers.length; i++) {
                    if(headers[i].length == 2) {
                        connection.setRequestProperty(headers[i][0], headers[i][1]);
                    } else {
                        System.err.println("executeHttpRequest(): The header size must be 2");
                        throw new Exception();
                    }
                }
            }

            connection.setRequestMethod(method);
            connection.setRequestProperty("Content-Type",
                    "application/x-www-form-urlencoded");

            connection.setRequestProperty("Content-Length",
                    Integer.toString(urlParameters.getBytes().length));
            connection.setRequestProperty("Content-Language", "en-US");

            connection.setUseCaches(false);
            connection.setDoOutput(true);

            //Send request
            if (method.equals("POST")) {
                DataOutputStream wr = new DataOutputStream(
                        connection.getOutputStream());
                wr.writeBytes(urlParameters);
                wr.close();
            }

            //Get Response
            InputStream is = connection.getInputStream();
            BufferedReader rd = new BufferedReader(new InputStreamReader(is));
            StringBuilder response = new StringBuilder();
            String line;
            while ((line = rd.readLine()) != null) {
                response.append(line);
                response.append('\r');
            }
            rd.close();
            return response.toString();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }

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
        String filePath = "C:\\Users\\Xylian\\Pictures\\city.list.json";
        try {
            String content = new String(Files.readAllBytes(Paths.get(filePath)));

            Gson gson = new Gson();

            Geo[] datasets = gson.fromJson(content, Geo[].class);

            for (Geo dataset : datasets) {
                System.out.println(dataset.coord.lat);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
