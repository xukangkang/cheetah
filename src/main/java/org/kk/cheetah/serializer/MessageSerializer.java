package org.kk.cheetah.serializer;

import java.io.IOException;

public interface MessageSerializer {
    public <T> T deserialize(byte[] bytes) throws IOException, ClassNotFoundException;

    public byte[] serialize(Object obj) throws IOException;

}
