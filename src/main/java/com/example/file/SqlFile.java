package com.example.file;

import lombok.Data;

import java.io.*;

@Data
public class SqlFile {
    String name;

    public SqlFile(String name) {
        this.name = name;
    }

    public String readSql() {

        StringBuilder sb = new StringBuilder();
        try {
            FileReader reader = new FileReader(this.name);
            BufferedReader bufferedReader = new BufferedReader(reader);
            String line = "";
            while (line != null) {
                sb.append(line);
                line = bufferedReader.readLine();
            }
        } catch (FileNotFoundException fnfe) {
            System.out.println(fnfe);
        } catch (IOException ioe) {
            System.out.println(ioe);
        }

        return sb.toString();
    }
}
