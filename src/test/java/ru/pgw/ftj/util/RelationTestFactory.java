package ru.pgw.ftj.util;


import ru.pgw.ftj.join.relation.IncompatibleNumberOfElementsException;
import ru.pgw.ftj.join.relation.TableRelation;

public final class RelationTestFactory {

    public static TableRelation createTableBooks() {
        TableRelation relation = new TableRelation("Books",
            "books_id", "title", "year", "customer_id", "source");
        try {
            relation.addTuple(1, "DBIS bestsellers", 101, 11, "DBIS");
            relation.addTuple(2, "New generation of scientists", 26, 4, "ADBIS");
            relation.addTuple(3, "SEIM conference 2022", 3, 2, "SEIM");
            relation.addTuple(4, "IEEE proceedings", 20, 12, "IEEE");
            relation.addTuple(5, "Bookvoed bestsellers", 1, 9, "BOOKVOED");
            relation.addTuple(6, "Zinger house", 1703, 812, "ZINGER");
            relation.addTuple(7, "SPBU", 1724, 812, "UNIVERSITY");
            relation.addTuple(8, "SPB POLYTECH conferences 2022", 1899, 812, "UNIVERSITY");
            relation.addTuple(9, "MSU diplomas 2015", 1756, 495, "UNIVERSITY");
            relation.addTuple(10, "IMATRA LIDL", 1000, 100500, "COUNTRY");
        } catch (IncompatibleNumberOfElementsException e) {
            e.printStackTrace();
        }

        return relation;
    }

    public static TableRelation createTableCustomers() {
        TableRelation relation = new TableRelation("Customers",
            "c_id", "name", "city", "number_of_employees");
        try {
            relation.addTuple(11, "DBIS Conference", "Riga", 55);
            relation.addTuple(4, "ADBIS Europe", "Italy", 100);
            relation.addTuple(2, "SEIM Conference", "Saint-Petersburg", 50);
            relation.addTuple(12, "IEEE World-wide", "PLANET", 1000);
            relation.addTuple(9, "Bookvoed", "Saint-Petersburg", 1000);
            relation.addTuple(812, "Saint-Petersburg education", "Saint-Petersburg", 1_000_000);
            relation.addTuple(495, "Moscow education", "Moscow", 1_000_000);
            relation.addTuple(496, "Finland groceries", "Imatra", 50000);
        } catch (IncompatibleNumberOfElementsException e) {
            e.printStackTrace();
        }

        return relation;
    }
}
