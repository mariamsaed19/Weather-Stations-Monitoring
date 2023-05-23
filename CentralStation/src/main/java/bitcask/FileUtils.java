package bitcask;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FileUtils {
    public FileUtils(){}
    public static void main(String[] args) throws Exception {
        String x = "bitcask/2023-05-22 12:47:53.802";
        String y ="bitcask/2023-05-22 12:47:53.802";
        System.out.println(getOldFiles("bitcask","2023-05-22 12:54:12.498").toString());

    }

//    /**
//     * lists files in a directory, sorted and excluding one file
//     * @param directory from which to list files
//     * @param excluded file to excluded from the returned list
//     * @return
//     */
//    public static List<String> getFilteredList(String directory, String excluded){
//        //list files in compaction directory -> sorted, exclude active
//        List<String> fileList;
//        try (Stream<Path> paths = Files.list(Paths.get(directory))) {
//            fileList = paths.filter(path -> !path.getFileName().toString().equals(excluded))
//                    .filter(Files::isRegularFile)
//                    .sorted(Comparator.comparing(Path::toString))
//                    .map(Path::toString)
//                    .collect(Collectors.toList());
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//        return fileList;
//    }

    public static List<String> getFilesList(String directory){
        return getFilteredList(directory,"");
    }

    public static List<String> getCondFiles(String directory, String cond,Comparator<String> comparator){
        List<String> fileList;
        try (Stream<Path> paths = Files.list(Paths.get(directory))) {
            fileList = paths.filter(path -> comparator.compare(path.getFileName().toString(),cond)!=0)
                    .filter(Files::isRegularFile)
                    .sorted(Comparator.comparing(Path::toString))
                    .map(Path::toString)
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return fileList;
    }

    public static List<String> getRecentFiles(String directory, String cond){
        return getCondFiles(directory,cond, (o1, o2) -> o1.compareTo(o2) > 0? 1 : 0);
    }
    public static List<String> getOldFiles(String directory, String cond){
        return getCondFiles(directory,cond, (o1, o2) -> o1.compareTo(o2) < 0? 1 : 0);
    }

    public static List<String> getFilteredList(String directory, String excluded){
        return getCondFiles(directory,excluded, (o1, o2) -> o1.compareTo(o2)!= 0? 1 : 0);
    }
    /**
     * writes compacted contents into a compacted temp file
     * @param content hashmap of contents
     * @param tempFile
     * @param fileID target compacted file ID
     * @return
     */
    public static Map<Long, KeydirEntity> createCompactedFile(Map<Long, byte[]> content, String tempFile, String fileID){
        Map<Long, KeydirEntity> updatedHashtable = new HashMap<>();
        try (DataOutputStream outputStream = new DataOutputStream(new FileOutputStream(tempFile))) {
            for (Map.Entry<Long, byte[]> entry : content.entrySet()) {
                Long key = entry.getKey();
                byte[] msg = entry.getValue();

                //write to hash table
                updatedHashtable.put(key, new KeydirEntity(fileID, outputStream.size()));

                //write msg to compacted file
                int msgSize = msg.length;
                outputStream.writeLong(key);
                outputStream.writeInt(msgSize);
                outputStream.write(msg);

            }
            outputStream.flush();
        } catch (IOException e) {
            System.err.println("Error writing binary file: " + e.getMessage());
        }
        return updatedHashtable;
    }

    /**
     * writes contents of keydir to hint file
     * @param keydir hashtable of compacted files
     * @param hintFile path to hint file
     */
    public static void createHintFile(Map<Long, KeydirEntity> keydir, String hintFile){
        try (DataOutputStream outputStream = new DataOutputStream(new FileOutputStream(hintFile))) {
            for (Map.Entry<Long, KeydirEntity> entry : keydir.entrySet()) {
                Long key = entry.getKey();
                KeydirEntity keydirEntity = entry.getValue();
                byte[] fileIDBytes = keydirEntity.getFileID().getBytes();

                //write to hint file: key, len(fileIDBytes), fileIDBytes, pos
                outputStream.writeLong(key);
                outputStream.writeInt(fileIDBytes.length);
                outputStream.write(fileIDBytes);
                outputStream.writeInt(keydirEntity.getValuePos());
            }
            outputStream.flush();
        } catch (IOException e) {
            System.err.println("Error writing binary file: " + e.getMessage());

        }
    }

    public static void deleteFiles(List<String> files){
        for(String fname : files){
            File file = new File(fname);
            if(!file.delete()){
                System.err.println("Failed to delete " + file);
            }
        }
    }

    public static void renameFile(String oldName, String newName){
        File file = new File(oldName);
        File newFile = new File(newName);
        if(!file.renameTo(newFile)){
            System.err.println("Failed to rename " + file + " to " + newFile);
        }
    }
}
