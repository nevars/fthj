package ru.pgw.ftj.caches;

import java.io.Serializable;
import java.util.Date;

public class LineItem implements Serializable {

    private int l_partkey;

    private float l_extendedprice;

    private float l_discount;

    private Date l_shipdate;

    private Integer l_orderkey;

    public LineItem() {
    }

    public LineItem(int l_partkey, int l_orderkey,
        float l_extendedprice, float l_discount, Date l_shipdate) {
        this.l_partkey = l_partkey;
        this.l_orderkey = l_orderkey;
        this.l_extendedprice = l_extendedprice;
        this.l_discount = l_discount;
        this.l_shipdate = l_shipdate;
    }

}
