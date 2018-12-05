package org.kk.cheetah.serializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.jboss.marshalling.Marshaller;
import org.jboss.marshalling.MarshallerFactory;
import org.jboss.marshalling.Marshalling;
import org.jboss.marshalling.MarshallingConfiguration;
import org.jboss.marshalling.Unmarshaller;
import org.kk.cheetah.handler.ProducerRecordRequestHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MarshallingSerializer implements MessageSerializer {
    private static Logger logger = LoggerFactory.getLogger(ProducerRecordRequestHandler.class);

    private static MarshallingConfiguration marshallingConfiguration = new MarshallingConfiguration();
    private static MarshallerFactory marshallerFactory;
    static {
        marshallingConfiguration.setVersion(5);
        marshallerFactory = Marshalling
                .getProvidedMarshallerFactory("serial");
    }

    @Override
    public <T> T deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
        Unmarshaller unmarshaller = marshallerFactory.createUnmarshaller(marshallingConfiguration);
        unmarshaller.start(Marshalling.createByteInput(byteArrayInputStream));
        Object object = unmarshaller.readObject();
        unmarshaller.finish();
        return (T) object;
    }

    @Override
    public byte[] serialize(Object obj) throws IOException {
        Marshaller marshaller = marshallerFactory.createMarshaller(marshallingConfiguration);
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        marshaller.start(Marshalling.createByteOutput(byteArrayOutputStream));
        marshaller.writeObject(obj);
        marshaller.finish();
        return byteArrayOutputStream.toByteArray();
    }

}
