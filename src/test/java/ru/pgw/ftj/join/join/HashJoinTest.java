package ru.pgw.ftj.join.join;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.pgw.ftj.join.FTHashJoin;
import ru.pgw.ftj.join.HashJoin;
import ru.pgw.ftj.join.Join;
import ru.pgw.ftj.join.relation.Relation;
import ru.pgw.ftj.join.relation.TableRelation;
import ru.pgw.ftj.util.RelationTestFactory;

@ExtendWith(MockitoExtension.class)
class HashJoinTest {

    Join join;

    TableRelation books;

    TableRelation customers;

    @Test
    void test_simpleHashJoin() {
        books = RelationTestFactory.createTableBooks();
        customers = RelationTestFactory.createTableCustomers();

        join = new HashJoin("temp",
            books, customers, "customer_id", "c_id");
        Relation temp = join.execute();
        assertThat(temp).isNotNull();
        assertThat(temp.numberOfTuples()).isEqualTo(8);
    }

    @Test
    void givenJoinCondition_whenFaultTolerantJoin_thenSuccess() {
        books = RelationTestFactory.createTableBooks();
        customers = RelationTestFactory.createTableCustomers();

        join = new FTHashJoin("temp",
            books, customers, "customer_id", "c_id");
        Relation temp = join.execute((book, customer) -> (int) book.get("year") > 1700);
        assertThat(temp).isNotNull();
        assertThat(temp.numberOfTuples()).isEqualTo(4);
    }

    @Test
    void givenTrueJoinCondition_whenFaultTolerantJoin_thenSuccess() {
        books = RelationTestFactory.createTableBooks();
        customers = RelationTestFactory.createTableCustomers();

        join = new FTHashJoin("temp",
            books, customers, "customer_id", "c_id");
        Relation temp = join.execute((book, customer) -> true);
        assertThat(temp).isNotNull();
        assertThat(temp.numberOfTuples()).isEqualTo(9);
    }

}