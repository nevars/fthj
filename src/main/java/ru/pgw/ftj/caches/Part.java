package ru.pgw.ftj.caches;

import java.io.Serializable;

public class Part implements Serializable {

    private int p_partkey;

    private String p_name;

    private String p_type;

    public Part() {
    }

    public Part(int p_partkey, String p_name) {
        this.p_partkey = p_partkey;
        this.p_name = p_name;
    }

    public Part(int p_partkey, String p_name, String p_type) {
        this(p_partkey, p_name);
        this.p_type = p_type;
    }

    public int getP_partkey() {
        return p_partkey;
    }

    public void setP_partkey(int p_partkey) {
        this.p_partkey = p_partkey;
    }

    public String getP_name() {
        return p_name;
    }

    public void setP_name(String p_name) {
        this.p_name = p_name;
    }

    public String getP_type() {
        return p_type;
    }

    public void setP_type(String p_type) {
        this.p_type = p_type;
    }

}
