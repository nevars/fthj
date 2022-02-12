package ru.pgw.ftj.join.relation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class TableRelation implements Relation {

    private final String name;

    private List<String> attributes;

    private List<Tuple> tuples;

    public TableRelation(String name, List<String> attributes) {
        this.name = name;
        this.attributes = attributes;
        tuples = new ArrayList<>();
    }

    public TableRelation(String name, String... attributeNames) {
        this.name = name;
        this.attributes = Arrays.stream(attributeNames).collect(Collectors.toList());
        tuples = new ArrayList<>();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public List<String> getAttributes() {
        return attributes;
    }

    @Override
    public String getAttribute(String attributeName) {
        for (String att : attributes) {
            if (att.equals(attributeName)) {
                return att;
            }
        }
        return null;
    }

    @Override
    public boolean addTuple(Tuple t) {
        if (t.size() != attributes.size()) {
            return false;
        }

        return tuples.add(t);
    }


    @Override
    public boolean addTuple(Object... elementValues) throws IncompatibleNumberOfElementsException {
        return addTuple(new Tuple(this, elementValues));
    }

    @Override
    public boolean removeTuple(Tuple t) throws IncompatibleNumberOfElementsException {
        if (t.size() != attributes.size()) {
            throw new IncompatibleNumberOfElementsException();
        }

        return tuples.remove(t);
    }

    @Override
    public int numberOfTuples() {
        return tuples.size();
    }

    @Override
    public Iterator<Tuple> iterator() {
        return tuples.iterator();
    }

    @Override
    public String toString() {
        StringBuilder relationTuples = new StringBuilder(2 * this.tuples.size() + 5);
        relationTuples.append(name).append(":\n");
        for (String attribute : attributes) {
            relationTuples.append(attribute).append("\t");
        }
        relationTuples.append("\n");
        for (Tuple tuple : tuples) {
            for (String attribute : attributes) {
                relationTuples.append(tuple.get(attribute)).append("\t");
            }
            relationTuples.append("\n");
        }
        return relationTuples.toString();
    }

}
