package ru.pgw.ftj.caches;

import java.io.Serializable;

public class PartSupp implements Serializable {

    private int ps_partkey;

    private String ps_dummy;

    public PartSupp(int ps_partkey, String ps_dummy) {
        this.ps_partkey = ps_partkey;
        this.ps_dummy = ps_dummy;
    }

    public int getPs_partkey() {
        return ps_partkey;
    }

    public void setPs_partkey(int ps_partkey) {
        this.ps_partkey = ps_partkey;
    }

    public String getPs_dummy() {
        return ps_dummy;
    }

    public void setPs_dummy(String ps_dummy) {
        this.ps_dummy = ps_dummy;
    }

}
