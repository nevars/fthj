package ru.pgw.ftj.join;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;
import ru.pgw.ftj.join.relation.Relation;
import ru.pgw.ftj.join.relation.TableRelation;
import ru.pgw.ftj.join.relation.Tuple;

public class HashJoin extends Join {

	public HashJoin(String outputName, Relation r1, Relation r2, String r1JoinAttribute, String r2JoinAttribute) {
		super(outputName, r1, r2, r1JoinAttribute, r2JoinAttribute);
	}

	@Override
	public Relation execute() {
		List<String> outputAttributes = new ArrayList<>(build.getAttributes());
		outputAttributes.addAll(probe.getAttributes());
		
		TableRelation output = new TableRelation(outputName, outputAttributes);
		
		Map<Object, Tuple> leftTable = new HashMap<Object, Tuple>();

		for (Tuple current : build) {
			leftTable.put(current.get(buildJoinAttribute), current);
		}
		probe(output, leftTable);
		
		return output;
	}

	@Override
	public Relation execute(BiPredicate<Tuple, Tuple> joinCondition) {
		return null;
	}

	private void probe(TableRelation output, Map<Object, Tuple> leftTableHashJoin) {
		for (Tuple rightTableTuple : probe) {
			if (leftTableHashJoin.containsKey(rightTableTuple.get(probeJoinAttribute))) {
				Tuple joinedTuple = new Tuple(leftTableHashJoin.get(rightTableTuple.get(probeJoinAttribute)));

				joinedTuple.putAll(rightTableTuple);
				output.addTuple(joinedTuple);
			}
		}
	}

}
