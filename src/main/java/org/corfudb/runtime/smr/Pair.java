package org.corfudb.runtime.smr;
import lombok.Data;

import java.io.Serializable;

//todo: custom serialization + unit tests
@Data
public class Pair<X, Y> implements Serializable
{
    public final X first;
    public final Y second;
    public Pair(X f, Y s)
    {
        first = f;
        second = s;
    }
}
