package ru.pgw.ftj.caches;

import java.io.Serializable;

public class Nation implements Serializable {

    private Integer n_nationkey;

    private String n_name;

    private Integer n_regionkey;

    private String n_comment;

    private String n_dummy;

    public Nation(Integer n_nationkey, String n_name, Integer n_regionkey, String n_comment, String n_dummy) {
        this.n_nationkey = n_nationkey;
        this.n_name = n_name;
        this.n_regionkey = n_regionkey;
        this.n_comment = n_comment;
        this.n_dummy = n_dummy;
    }

    public Integer getN_nationkey() {
        return n_nationkey;
    }

    public void setN_nationkey(Integer n_nationkey) {
        this.n_nationkey = n_nationkey;
    }

    public String getN_name() {
        return n_name;
    }

    public void setN_name(String n_name) {
        this.n_name = n_name;
    }

    public Integer getN_regionkey() {
        return n_regionkey;
    }

    public void setN_regionkey(Integer n_regionkey) {
        this.n_regionkey = n_regionkey;
    }

    public String getN_comment() {
        return n_comment;
    }

    public void setN_comment(String n_comment) {
        this.n_comment = n_comment;
    }

    public String getN_dummy() {
        return n_dummy;
    }

    public void setN_dummy(String n_dummy) {
        this.n_dummy = n_dummy;
    }

}
