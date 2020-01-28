package com.hadoop.hlsijx.model;

public class FileDetail {

    private String fileType;
    private String permission;
    private String replication;
    private String owner;
    private String group;
    private String path;


    public void setFileType(String fileType) {
        this.fileType = fileType;
    }

    public void setPermission(String permission) {
        this.permission = permission;
    }

    public void setReplication(String replication) {
        this.replication = replication;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public void setPath(String path) {
        this.path = path;
    }

    @Override
    public String toString() {
        return "FileDetail{" +
                "fileType='" + fileType + '\'' +
                ", permission='" + permission + '\'' +
                ", replication='" + replication + '\'' +
                ", owner='" + owner + '\'' +
                ", group='" + group + '\'' +
                ", path='" + path + '\'' +
                '}';
    }
}
