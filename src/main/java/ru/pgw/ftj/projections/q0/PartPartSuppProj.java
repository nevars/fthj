package ru.pgw.ftj.projections.q0;

import lombok.NonNull;
import lombok.Value;
import org.apache.ignite.binary.BinaryObject;
import ru.pgw.ftj.caches.Part;
import ru.pgw.ftj.caches.PartSupp;
import ru.pgw.ftj.failover.FaultTolerant;

/**
 * The projection of {@link Part} and {@link PartSupp}.
 */
@Value
public class PartPartSuppProj implements FaultTolerant {

    int partId;

    String surrogateKey;

    String name;

    String dummy;

    public PartPartSuppProj(String dummy) {
        this.surrogateKey = "d1";
        this.partId = 1;
        this.name = "n1";
        this.dummy = dummy;
    }

    public PartPartSuppProj(@NonNull BinaryObject partBinObj, String dummy) {
        this.partId = partBinObj.field("p_partkey");
        this.name = partBinObj.field("p_name");
        this.dummy = dummy;
        this.surrogateKey = "";
    }

    public PartPartSuppProj(@NonNull String surrogateKey, @NonNull BinaryObject partBinObj, String dummy) {
        this.surrogateKey = surrogateKey;
        this.partId = partBinObj.field("p_partkey");
        this.name = partBinObj.field("p_name");
        this.dummy = dummy;
    }

    @Override
    public String key() {
        return surrogateKey;
    }

}
