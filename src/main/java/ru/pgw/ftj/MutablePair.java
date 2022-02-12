package ru.pgw.ftj;

import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
public class MutablePair<L, R> implements Serializable {

    private L left;

    private R right;
}
