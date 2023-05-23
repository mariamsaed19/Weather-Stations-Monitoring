package bitcask;

public class KeydirEntity {
    private String fileID;
    private int valuePos;

    public KeydirEntity(String fileID, int valuePos) {
        this.fileID = fileID;
        this.valuePos = valuePos;
    }

    public String getFileID() {
        return fileID;
    }

    public void setFileID(String fileID) {
        this.fileID = fileID;
    }

    public int getValuePos() {
        return valuePos;
    }

    public void setValuePos(int valuePos) {
        this.valuePos = valuePos;
    }

}
