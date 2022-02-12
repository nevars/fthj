package ru.pgw.ftj.join;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;
import ru.pgw.ftj.join.relation.Relation;
import ru.pgw.ftj.join.relation.TableRelation;
import ru.pgw.ftj.join.relation.Tuple;

public class FTHashJoin extends Join {

    public FTHashJoin(String outputName, Relation r1, Relation r2, String r1JoinAttribute, String r2JoinAttribute) {
        super(outputName, r1, r2, r1JoinAttribute, r2JoinAttribute);
    }

    @Override
    public Relation execute() {
        return execute((t1, t2) -> true);
    }

    @Override
    public Relation execute(BiPredicate<Tuple, Tuple> joinCondition) {
        ArrayList<String> outputAttributes = new ArrayList<>(build.getAttributes());
        outputAttributes.addAll(probe.getAttributes());

        TableRelation output = new TableRelation(outputName, outputAttributes);
        Map<Object, List<Tuple>> leftTable = new HashMap<>();

        for (Tuple leftTableTuple : build) {
            leftTable.computeIfAbsent(
                leftTableTuple.get(buildJoinAttribute),
                l -> new ArrayList<>()).add(leftTableTuple);
        }

        // probe
        probe(joinCondition, output, leftTable);

        return output;
    }

    private void probe(BiPredicate<Tuple, Tuple> joinCondition, final TableRelation output,
        Map<Object, List<Tuple>> leftTable) {
        for (Tuple rightTableTuple : probe) {
            if (leftTable.containsKey(rightTableTuple.get(probeJoinAttribute))) {
                List<Tuple> leftTuples = leftTable.get(rightTableTuple.get(probeJoinAttribute));
                leftTuples.stream()
                    .filter(leftTuple -> joinCondition.test(leftTuple, rightTableTuple))
                    .forEach(leftTuple -> {
                        Tuple joinedTuple = new Tuple(leftTuple);
                        joinedTuple.putAll(rightTableTuple);
                        output.addTuple(joinedTuple);
                    });
            }
        }
    }

}
