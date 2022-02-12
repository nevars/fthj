package ru.pgw.ftj.join;

import java.util.function.BiPredicate;
import ru.pgw.ftj.join.relation.Relation;
import ru.pgw.ftj.join.relation.Tuple;

public abstract class Join {

    protected String outputName;

    protected Relation build;

    protected Relation probe;

    protected String buildJoinAttribute;

    protected String probeJoinAttribute;

    public Join(String outputName, Relation r1, Relation r2, String r1JoinAttribute, String r2JoinAttribute) {
        this.outputName = outputName;
        this.build = r1;
        this.probe = r2;
        this.buildJoinAttribute = r1JoinAttribute;
        this.probeJoinAttribute = r2JoinAttribute;
    }

    public abstract Relation execute();

    public abstract Relation execute(BiPredicate<Tuple, Tuple> joinCondition);

}
