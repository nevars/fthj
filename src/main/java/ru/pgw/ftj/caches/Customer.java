package ru.pgw.ftj.caches;

import java.io.Serializable;

public class Customer implements Serializable {

    private Integer c_custkey;

    private String c_name;

    private String c_address;

    private Integer c_nationkey;

    private String c_phone;

    private String c_mktsegment;

    private String c_comment;

    private String c_dummy;

    public Customer(Integer c_custkey, String c_name, String c_address, Integer c_nationkey, String c_phone,
        String c_mktsegment, String c_comment, String c_dummy) {
        this.c_custkey = c_custkey;
        this.c_name = c_name;
        this.c_address = c_address;
        this.c_nationkey = c_nationkey;
        this.c_phone = c_phone;
        this.c_mktsegment = c_mktsegment;
        this.c_comment = c_comment;
        this.c_dummy = c_dummy;
    }

    public Integer getC_custkey() {
        return c_custkey;
    }

    public void setC_custkey(Integer c_custkey) {
        this.c_custkey = c_custkey;
    }

    public String getC_name() {
        return c_name;
    }

    public void setC_name(String c_name) {
        this.c_name = c_name;
    }

    public String getC_address() {
        return c_address;
    }

    public void setC_address(String c_address) {
        this.c_address = c_address;
    }

    public Integer getC_nationkey() {
        return c_nationkey;
    }

    public void setC_nationkey(Integer c_nationkey) {
        this.c_nationkey = c_nationkey;
    }

    public String getC_phone() {
        return c_phone;
    }

    public void setC_phone(String c_phone) {
        this.c_phone = c_phone;
    }

    public String getC_mktsegment() {
        return c_mktsegment;
    }

    public void setC_mktsegment(String c_mktsegment) {
        this.c_mktsegment = c_mktsegment;
    }

    public String getC_comment() {
        return c_comment;
    }

    public void setC_comment(String c_comment) {
        this.c_comment = c_comment;
    }

    public String getC_dummy() {
        return c_dummy;
    }

    public void setC_dummy(String c_dummy) {
        this.c_dummy = c_dummy;
    }

}
