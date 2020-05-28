package main.java.setup;

import com.basho.riak.client.api.cap.ConflictResolver;
import com.basho.riak.client.api.cap.UnresolvedConflictException;
import com.basho.riak.client.core.query.RiakObject;

import java.util.List;

public class Resolver implements ConflictResolver<RiakObject> {

    @Override
    public RiakObject resolve(List<RiakObject> list) throws UnresolvedConflictException {
        for(RiakObject element: list)
        {
            System.out.println(element.getValue().toString());
        }
        return list.get(0);
    }
}
