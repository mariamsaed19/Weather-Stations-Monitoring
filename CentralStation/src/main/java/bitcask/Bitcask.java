package bitcask;

import org.json.JSONObject;
import sun.misc.Signal;
import sun.misc.SignalHandler;

import java.io.*;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Bitcask {

    private String workingDir;
    private String hintDir;
    public String activeFile;
    private DataOutputStream activeFileStream;
    private int countSegments;
    private int offset;
    public Map<Long, KeydirEntity> keydir;
    private final String KEY_NAME = "station_id";
    private final int MAX_SEGMENT_SIZE = 600; //TODO: set value
    private final int MAX_SEGMENT_COUNT = 10;
    private final int SLEEPING_PERIOD = 5000;
    private final ReadWriteLock keydirLock = new ReentrantReadWriteLock();
    private final ReadWriteLock activeFileLock = new ReentrantReadWriteLock();

    private static AtomicBoolean shuttingDown = new AtomicBoolean(false);

    private CompactionThread compactionThread ;
    public Bitcask(String workingDir) throws FileNotFoundException {
        this.workingDir = workingDir; //TODO: validate that the working directory exists
        this.hintDir = Paths.get(this.workingDir, "hint").toString(); //TODO: handle if directory doesn't exist

        this.countSegments = FileUtils.getFilesList(this.workingDir).size();

        this.activeFile = Paths.get(this.workingDir, new Timestamp(System.currentTimeMillis()).toString()).toString();
        this.activeFileStream = new DataOutputStream(new FileOutputStream(this.activeFile, true));
//        this.activeFile = "2023-05-16 14:47:43.791"; //TODO: remove

        System.out.println("initial count: "+this.countSegments);
        this.offset = 0;
        this.keydir = recover();
        this.safeClose();
        this.compactionThread = new CompactionThread();
        this.compactionThread.start();

    }

    private class CompactionThread extends Thread {

        @Override
        public void run(){
            /*
             * create and start compaction thread in bitcask constructor
             * compaction thread sleeps for m seconds
             * after thread wakes, it checks if it should perform compaction
             * compaction thread sleeps if no enough files to compact, and starts compaction if there is.
             * */
            while(true){
                // check if there is enough files to do compaction
                String currActiveFile = getActiveFileIfCompReady();
                if(currActiveFile!=null){
                    this.doCompaction(currActiveFile);
                }
                else{
                    System.out.println("Not Enough Files --> sleep ..");
                }
                try {
                    Thread.sleep(SLEEPING_PERIOD);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

            }
        }
        public void doCompaction(String currActiveFile){

            System.out.println("Start Compaction ...");

            //copy active file name
            String excludeFileName = currActiveFile.split("/")[1];
            //TODO: get older files and exclude curr file -- done --
            //list files in compaction directory -> sorted, exclude active
            List<String> compFileList = FileUtils.getOldFiles(workingDir, excludeFileName);
            System.out.println("excluded: "+excludeFileName+" Current complist "+compFileList.toString());

            //create temp hash table
            Map<Long, byte[]> tempHashtable = new HashMap<>();

            //loop over files in order -> old to new
            //walk through single file -> update temp hash table
            for (String file : compFileList) {
                try (DataInputStream inputStream = new DataInputStream(new FileInputStream(file))) {
                    while(inputStream.available()>0) {
                        long key = inputStream.readLong();
                        int msgBytes = inputStream.readInt();
                        byte[] msg = new byte[msgBytes];
                        inputStream.read(msg);
                        tempHashtable.put(key, msg);
                    }
                } catch (IOException e) {
                    System.err.println("Error reading binary file: " + e.getMessage());
                }
            }

            //create compacted file
            String timestamp = compFileList.get(compFileList.size()-1).split("/")[1];
            String compactedFile = Paths.get(workingDir, timestamp).toString();
            String tempCompFile = Paths.get(workingDir, "temp").toString();
            Map<Long, KeydirEntity> updatedHashtable = FileUtils.createCompactedFile(tempHashtable, tempCompFile, compactedFile);

            //create hint file (in hint file directory) -> write temp hashtable
            String tempHintFile = Paths.get(hintDir, "temp").toString();
            FileUtils.createHintFile(updatedHashtable, tempHintFile);

            //clear & replace compacted
            FileUtils.deleteFiles(compFileList);
            FileUtils.renameFile(tempCompFile, compactedFile);

            //clear & replace hint
            List<String> hintFileList = FileUtils.getFilteredList(hintDir, "temp");
            FileUtils.deleteFiles(hintFileList);
            String hintFile = Paths.get(hintDir, timestamp).toString();
            FileUtils.renameFile(tempHintFile, hintFile);

            updateKeydir(updatedHashtable);

            //TODO: remove this part -- done --
            incrementCountSegment();
            //updateCountSegments(-1*(compFileList.size()-1));

        }
        private void updateKeydir(Map<Long, KeydirEntity> updatedHashtable){
            keydirLock.writeLock().lock();
            try {
                //update original hash table
                for (Map.Entry<Long, KeydirEntity> entry : keydir.entrySet()) {
                    Long key = entry.getKey();
                    KeydirEntity orgKeydirEntity = entry.getValue();
                    KeydirEntity updatedKeydirEntity = updatedHashtable.get(key);
//                    System.out.println("begin ");
//                    System.out.println(orgKeydirEntity+"  "+updatedKeydirEntity);

                    if (updatedKeydirEntity!=null && orgKeydirEntity.getFileID().compareTo(updatedKeydirEntity.getFileID()) > 0) {
                        updatedHashtable.put(key, orgKeydirEntity);
                    }
                }
                keydir = updatedHashtable;
            }finally {
                keydirLock.writeLock().unlock();
            }
        }
    }

    public static void main(String[] args) throws FileNotFoundException {

//        String jsonString = "{\n" +
//                "    \"station_id\": 5,\n" +
//                "    \"s_no\": 1,\n" +
//                "    \"battery_status\": \"low\",\n" +
//                "    \"status_timestamp\": 1681521224,\n" +
//                "    \"weather\": {\n" +
//                "        \"humidity\": 35,\n" +
//                "        \"temperature\": 100,\n" +
//                "        \"wind_speed\": 13\n" +
//                "    }\n" +
//                "}" ;
//        JSONObject obj = new JSONObject(jsonString);

        String jsonString2 = "{\n" +
                "    \"station_id\": 6,\n" +
                "    \"s_no\": 1,\n" +
                "    \"battery_status\": \"low\",\n" +
                "    \"status_timestamp\": 1681521224,\n" +
                "    \"weather\": {\n" +
                "        \"humidity\": 35,\n" +
                "        \"temperature\": 100,\n" +
                "        \"wind_speed\": 13\n" +
                "    }\n" +
                "}" ;
        JSONObject obj2 = new JSONObject(jsonString2);
       // System.out.println(obj.keySet());

        Bitcask bitcask = new Bitcask("bitcask");
        bitcask.showContent();

//        bitcask.doCompaction();
        // creates 4 files
        for (int i = 0; i < 500; i++) {
            System.out.println("i: "+i);
            String jsonString = "{\n" +
                    "    \"station_id\":"+i+",\n" +
                    "    \"s_no\": 1,\n" +
                    "    \"battery_status\": \"low\",\n" +
                    "    \"status_timestamp\": 1681521224,\n" +
                    "    \"weather\": {\n" +
                    "        \"humidity\": 55,\n" +
                    "        \"temperature\": 100,\n" +
                    "        \"wind_speed\": 13\n" +
                    "    }\n" +
                    "}" ;
            JSONObject obj = new JSONObject(jsonString);
           bitcask.update(obj);
           if(i%10==0&&i>0){
               try {
                   Thread.sleep(4000);
               } catch (InterruptedException e) {
                   throw new RuntimeException(e);
               }
           }
           //bitcask.update(obj2);

        }
        String jsonString3 = "{\n" +
                "    \"station_id\": 7,\n" +
                "    \"s_no\": 1,\n" +
                "    \"battery_status\": \"low\",\n" +
                "    \"status_timestamp\": 1681521224,\n" +
                "    \"weather\": {\n" +
                "        \"humidity\": 35,\n" +
                "        \"temperature\": 100,\n" +
                "        \"wind_speed\": 13\n" +
                "    }\n" +
                "}" ;
       // JSONObject obj3 = new JSONObject(jsonString3);
        //bitcask.update(obj3);
        //bitcask.showContent();

//        try (DataInputStream inputStream = new DataInputStream(new FileInputStream(bitcask.activeFile))) {
//            long key = inputStream.readLong();
//            int msgBytes = inputStream.readInt();
//            byte[] msg = new byte[msgBytes];
//            inputStream.read(msg);
//            System.out.println("key = " + key + ", msg len = " + msgBytes + "\nmsg: " + new String(msg));
//        } catch (IOException e) {
//            System.err.println("Error reading binary file: " + e.getMessage());
//        }

       // System.out.println("record: "+bitcask.readRecord(1l).getJSONObject("weather"));

        //for(Map.Entry<Long, KeydirEntity> entry : bitcask.keydir.entrySet()){
          //  System.out.println("key = " + entry.getKey() + ", fileID = " + entry.getValue().getFileID() + ", offset = " + entry.getValue().getValuePos());
        //}
    }
    public void safeClose(){
        // Add a shutdown hook to close the output stream
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                if (!shuttingDown.getAndSet(true)) {
                    try {
                        System.out.println("Shut down through hook ...");
                        activeFileStream.close();
                    } catch (IOException e) {
                        // Handle the exception
                        System.err.println("Error cannot close file: " + e.getMessage());
                    }
                }
            }
        });

        // Install a signal handler to detect termination signals
        Signal.handle(new Signal("INT"), new SignalHandler() {
            @Override
            public void handle(Signal signal) {
                System.out.println("Received signal " + signal.getName());
                System.out.println("Shut down through Signal ...");
                if (!shuttingDown.getAndSet(true)) {
                    try {
                        activeFileStream.close();
                    } catch (IOException e) {
                        // Handle the exception
                        System.err.println("Error cannot close file: " + e.getMessage());
                    }
                }
                Runtime.getRuntime().halt(0);
            }
        });
    }
    //TODO: remove this -- done --
//    public String getActiveFile(){
//        activeFileLock.readLock().lock();
//        try{
//            return this.activeFile;
//        }
//        finally {
//            activeFileLock.readLock().unlock();
//        }
//    }
    public Map<Long, KeydirEntity> getKeydir(){
        keydirLock.readLock().lock();
        try{
            return keydir;
        }
        finally {
            keydirLock.readLock().unlock();
        }
    }

    public KeydirEntity getKeydirEntity(Long key){
        keydirLock.readLock().lock();
        try{
            return this.keydir.get(key);
        }
        finally {
            keydirLock.readLock().unlock();
        }
    }
    //TODO: remove this and add "get activefile & decrement " method
    public String getActiveFileIfCompReady(){
        activeFileLock.writeLock().lock();
        try{
            if (this.countSegments >= MAX_SEGMENT_COUNT) {
                this.countSegments = 0;
                return this.activeFile;
            }
            return null;

        }finally {
            activeFileLock.writeLock().unlock();
        }
    }
    public void incrementCountSegment(){
        activeFileLock.writeLock().lock();
        try{
            this.countSegments++;

        }finally {
            activeFileLock.writeLock().unlock();
        }
    }
//    public int getCountSegments(){
//        countSegmentLock.readLock().lock();
//        try{
//            return this.countSegments;
//        }
//        finally {
//            countSegmentLock.readLock().unlock();
//        }
//    }

    //TODO: modify to increment count --done--
    public void switchActiveFile(String newActiveFile){
        activeFileLock.writeLock().lock();
        try{
            this.activeFile = newActiveFile;
            this.activeFileStream.close();
            this.activeFileStream = new DataOutputStream(new FileOutputStream(this.activeFile, true));
            this.countSegments ++;

        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            activeFileLock.writeLock().unlock();
        }
    }

    public void putKeydirEntity(Long key ,KeydirEntity entity){
        keydirLock.writeLock().lock();
        try{
            this.keydir.put(key,entity);
        }
        finally {
            keydirLock.writeLock().unlock();
        }
    }
    //TODO: remove this -- done --
//    public void updateCountSegments(int count){
//        countSegmentLock.writeLock().lock();
//        try{
//            this.countSegments += count;
//        }
//        finally {
//            countSegmentLock.writeLock().unlock();
//        }
//    }
    public JSONObject readRecord(Long key){
        KeydirEntity entity = getKeydirEntity(key);
        String fileID = entity.getFileID();
        String msgContent="";
        try (DataInputStream inputStream = new DataInputStream(new FileInputStream(fileID))) {
            inputStream.skipBytes(entity.getValuePos());
            inputStream.readLong(); // skip key
            int msgLength = inputStream.readInt();
            byte[] msg = new byte[msgLength];
            //System.out.println("FileId: "+fileID+" msglen: "+msgLength+" available: "+inputStream.available());
            inputStream.read(msg);
            msgContent = new String(msg);
        } catch (IOException e) {
            System.err.println("Error reading binary file: " + e.getMessage());
        }
        System.out.println("msg: "+msgContent);
        return new JSONObject(msgContent);
    }

    public void update(JSONObject msg){
        writeToActiveFile(msg);
        //doCompaction();
    }

    public void showContent(){
        List<List<String>> rows = new ArrayList<>();
        Map<Long, KeydirEntity> currKeydir = getKeydir();
        for (Long key : currKeydir.keySet()){
            JSONObject record = this.readRecord(key);
            Map<String,Object> row = PrintUtils.getJsonFields(record);
            if(rows.size()==0){
                rows.add(new ArrayList<>(row.keySet()));
            }
            List<String> rowContent = new ArrayList<>();
            for (String val : row.keySet()) {
                rowContent.add(row.get(val).toString());
            }
            rows.add(rowContent);
        }
        PrintUtils.showTable(rows);
    }

    public void writeToActiveFile(JSONObject msg){

        Long key = msg.getLong(KEY_NAME);
        int recordSize = 0;

        // write msg to active file

        byte[] msgBytes = msg.toString().getBytes();
        int msgSize = msgBytes.length;
        try {
            this.activeFileStream.writeLong(key);
            this.activeFileStream.writeInt(msgSize);
            this.activeFileStream.write(msgBytes);
            recordSize = Long.BYTES + Integer.BYTES + msgSize;

            this.activeFileStream.flush();
        } catch (IOException e) {
            System.err.println("Error reading binary file: " + e.getMessage());
        }



        // update keydir hashtable
        this.putKeydirEntity(key,new KeydirEntity(this.activeFile, this.offset));

        // update offset & active file
        this.offset += recordSize;
        if(this.offset > MAX_SEGMENT_SIZE){ //switch to new segment

            //TODO: use only setActiveFile to increment the count and switch file -- done --
            this.switchActiveFile(Paths.get(this.workingDir, new Timestamp(System.currentTimeMillis()).toString()).toString());
            System.out.println("Add new segment ..");
            this.offset = 0;
        }
    }


//    private void doCompaction(){
//        //TODO: use method to get active file and decrement count -- done --
//        String currActiveFile = this.getActiveFileIfCompReady();
//        if (currActiveFile == null)
//            return;
//
//        //TODO: send active file name -- done --
//        System.out.println("Compaction is ready ..."+currActiveFile);
//        CompactionThread comThread = new CompactionThread(currActiveFile);
//        comThread.start();
//
//
//    }



    public Map<Long, KeydirEntity> recoverFromHint(List<String> hintFiles){

        Map<Long,KeydirEntity> recoveredKeydir = new HashMap<>();

        for( String hintFile : hintFiles){
            try (DataInputStream inputStream = new DataInputStream(new FileInputStream(hintFile))) {
                while(inputStream.available()>0) {
                    long key = inputStream.readLong();
                    int fileIDLength = inputStream.readInt();
                    byte[] fileIDBytes = new byte[fileIDLength];
                    inputStream.read(fileIDBytes);
                    String fileID = new String(fileIDBytes);
                    int valuePos = inputStream.readInt();

                    recoveredKeydir.put(key, new KeydirEntity(fileID, valuePos));
                }

            } catch (IOException e) {
                System.err.println("Error reading binary file: " + e.getMessage());
            }
        }

        return recoveredKeydir;
    }

    public Map<Long, KeydirEntity> recover(){
        System.out.println("Start recovery ...");
        List<String> hintFiles = FileUtils.getFilesList(this.hintDir);
        Map<Long,KeydirEntity> recovered = recoverFromHint(hintFiles);
        List<String> recentActiveFiles;
        if (hintFiles.size()==0){
            recentActiveFiles = FileUtils.getFilesList(this.workingDir);
        }
        else{
            String cond = new File(hintFiles.get(hintFiles.size()-1)).getName();
            //TODO: delete System.out.println("482 :"+cond);
            recentActiveFiles = FileUtils.getRecentFiles(this.workingDir,cond);
            //TODO: delete  System.out.println("Hint file name: "+cond+" Recent list "+recentActiveFiles.toString());
        }

        for( String recentFile : recentActiveFiles){

            try (DataInputStream inputStream = new DataInputStream(new FileInputStream(recentFile))) {
                int pos = 0;
                while(inputStream.available()>0) {
                    long key = inputStream.readLong();
                    int msgBytes = inputStream.readInt();
                    byte[] msg = new byte[msgBytes];
                    inputStream.read(msg);
                    recovered.put(key,new KeydirEntity(recentFile,pos));
                    pos += Long.BYTES + Integer.BYTES + msgBytes;
                }

            } catch (IOException e) {
                System.err.println("Error reading binary file: " + e.getMessage());
            }
        }



        return recovered;
    }

}
