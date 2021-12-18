package efimov483.labs;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.ArrayList;

public class DBHelper {
    public static final String URL = "jdbc:mysql://localhost:3306/lab04";
    public static final String USERNAME = "user";
    public static final String PASSWORD = "1234";

    public static void main(String[] args) throws IOException {
        String path = "C:\\Users\\Imertvec\\Desktop\\elements";
        ArrayList<String> elements = getFiles(path);

        try (Connection connection = DriverManager.getConnection(URL, USERNAME, PASSWORD)) {
            System.out.println("Database connected!");
            Statement stmt = connection.createStatement();

            //add tables with elements name
            for(String el : elements){
                String sql = "CREATE TABLE " + el +
                        "(wave_lenght FLOAT, rel_intensity FLOAT);";
                //stmt.execute(sql);
            }

            //get files content & distribute to 2 columns
            File folder = new File(path);
            File[] files = folder.listFiles();
            for(File file : files){
                if(file.isFile()){

                    //get all file's content
                    ArrayList<String> rows = (ArrayList<String>) Files.readAllLines(Paths.get(file.getAbsolutePath()));
                    String fileName = file.getName().substring(0, file.getName().length() - 4);

                    //create quary-line per element
                    String sql1 = "INSERT INTO " + fileName + " (wave_lenght, rel_intensity) VALUES ";
                    for(int i = 0; i < rows.size(); i++){
                        String[] col = rows.get(i).split(" ");
                        sql1 += "(" + col[0] + ", " + col[1] + ")";

                        if(i < rows.size() - 1) sql1 += ", ";
                    }
                    sql1 += ";";
                    System.out.println(sql1);
                    //stmt.execute(sql1);
                }
            }

            //fill table spectral_lines
            String sql3 = "SELECT atomic_number, full_name FROM elements;";
            ResultSet atNum_Name = stmt.executeQuery(sql3);

            String sql4 = "";
            while (atNum_Name.next()){
                int atomic_number = atNum_Name.getInt("atomic_number");
                String full_name = atNum_Name.getString("full_name");

                //create quary with atomic_number & full_name
                sql4 += "INSERT INTO spectral_lines ";
                sql4 += "(atomic_number, wave_lenght, rel_intensity) ";
                sql4 += "SELECT " + atomic_number + " AS atomic_number, ";
                sql4 += "(wave_lenght * 0.1) AS wave_lenght, rel_intensity FROM " + full_name + ";\n";
            }

            //stmt.execute(sql4);   //for some reason query does not work
            System.out.println(sql4);

        } catch (SQLException e) {
            e.printStackTrace();
        }

        System.out.println("Database disconnected!");
    }

    private static ArrayList<String> getFiles(String path) throws IOException {
        File folder = new File(path);
        File[] listOfFiles = folder.listFiles();
        ArrayList<String> elements = new ArrayList<>();
        if(listOfFiles != null){
            for (File file : listOfFiles) {
                if (file.isFile()) {
                    //get filename
                    String temp = file.getName();

                    //delete .txt from file
                    temp = temp.substring(0, temp.length() - 4);

                    //add to list
                    elements.add(temp);
                }
            }
        }

        return elements;
    }
}

