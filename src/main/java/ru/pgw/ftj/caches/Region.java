package ru.pgw.ftj.caches;

import java.io.Serializable;

public class Region implements Serializable {

    private Integer r_regionkey;

    private String r_name;

    private String r_comment;

    private String r_dummy;

    public Region(Integer r_regionkey, String r_name, String r_comment, String r_dummy) {
        this.r_regionkey = r_regionkey;
        this.r_name = r_name;
        this.r_comment = r_comment;
        this.r_dummy = r_dummy;
    }

    public Integer getR_regionkey() {
        return r_regionkey;
    }

    public void setR_regionkey(Integer r_regionkey) {
        this.r_regionkey = r_regionkey;
    }

    public String getR_name() {
        return r_name;
    }

    public void setR_name(String r_name) {
        this.r_name = r_name;
    }

    public String getR_comment() {
        return r_comment;
    }

    public void setR_comment(String r_comment) {
        this.r_comment = r_comment;
    }

    public String getR_dummy() {
        return r_dummy;
    }

    public void setR_dummy(String r_dummy) {
        this.r_dummy = r_dummy;
    }

}
