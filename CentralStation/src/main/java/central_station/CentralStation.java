package central_station;

import bitcask.Bitcask;
import org.json.JSONObject;

import java.io.FileNotFoundException;
/*
    - poll from kafka
    - insert into bitcask
    - buffer batch for parquet
    - spawn thread to write parquet

 */
public class CentralStation {
    public static void main(String[] args) throws FileNotFoundException {
        String workingDir = "bitcask";
        Bitcask bitcask = new Bitcask(workingDir);

        // recieve one message from kafka

        // validate message

        // update bitcask

        // add to batch

        // check batch size and spawn thread for writing
    }
}
