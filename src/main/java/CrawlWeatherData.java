import com.google.gson.Gson;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.ResultSet;
import java.sql.SQLException;

public class CrawlWeatherData {
    public static final String API_KEY_OPEN_WEATHER_MAP = "0748fd70f9msh3ed6f2f69059c2ap196176jsndaf369ea7081";

    public static void main(String[] args) {
        // DB init
        SQLConnection.init();

        ResultSet a = SQLConnection.query("SHOW DATABASES");
        try {
            while(a.next()) {
                System.out.println(a.getString("Database"));
            }
            System.out.print("\n");
        } catch (SQLException e) {
            e.printStackTrace();
        }

        String geo_city;
        double geo_lat, geo_lon;

        // crawl weather data: Open Weather Map
        String[][] headers = {
                {"X-RapidAPI-Host", "community-open-weather-map.p.rapidapi.com"},
                {"X-RapidAPI-Key", API_KEY_OPEN_WEATHER_MAP}
        };

        ResultSet device_rs = SQLConnection.query("SELECT ID, lon, lat FROM device");
        Gson gson = new Gson();
        try {
            while(device_rs.next()) {
                System.out.println("crawling..");

                //geo_city = "Hanover";
                geo_lat = device_rs.getDouble("lat");
                geo_lon = device_rs.getDouble("lon");

                String response = executeHttpRequest("https://community-open-weather-map.p.rapidapi.com/weather",
                        "&lat="+geo_lat
                                + "&lon="+geo_lon
                                + "&units=metric"
                                + "&mode=json",
                                //+ "&q="+geo_city,
                        "GET", headers);
                System.out.println(response);

                WeatherData weatherData = gson.fromJson(response, WeatherData.class);
                insertWeatherData(headers[0][1], geo_lon, geo_lat, weatherData.main.temp, weatherData.main.pressure, weatherData.main.humidity);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        SQLConnection.shutdown();
    }

    private class WeatherData {
        Main main;

        WeatherData(Main main) {
            this.main = main;
        }

        class Main {
            double temp, pressure, humidity;

            Main(double temp, double pressure, double humidity) {
                this.temp = temp;
                this.pressure = pressure;
                this.humidity = humidity;
            }
        }
    }

    private static void insertWeatherData(String source_address, double lon, double lat, double temp, double pressure, double humidity) {
        String sql = "INSERT INTO `weather`(`source_typ`, `source_address`, `lon`, `lat`, `temp`, `pressure`, `humidity`) "
                + "VALUES ("+ 1 +",'"+source_address+"','"+lon+"','"+lat+"',"
                + "'"+temp+"','"+pressure+"','"+humidity+"');";
        System.out.println("insertWeatherData(): " + sql);
        SQLConnection.update(sql);
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

}
