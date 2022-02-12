package ru.pgw.ftj.failover;

import java.io.Serializable;

public interface FaultTolerant extends Serializable {

    /**
     * @return Task ID + Job ID + partition ID.
     */
    String key();

}
