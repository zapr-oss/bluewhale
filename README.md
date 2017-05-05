Welcome to project BlueWhale
=============================

BlueWhale provides a lightweight in-memory cache which acts as a data container for various datasources. 

###What is BlueWhale?

BlueWhale is an embeddable key-value based cache populated automatically from data stores like MySQL, Aerospike and File. 
It has both thread-safe and unsafe versions.

###What is it suitable for?
BlueWhale provides fast key-value based lookup cache over persistent data stores, which needs to be refreshed periodically.

**Notes**:
1. It exposes unsafeUpdate function as well, which should be used only for those caches, where updates are almost non-existent or very rare (such as handset, geo etc), otherwise it may lead to inconsistency.
2. For updatable cache, we need to provision twice the memory. For frequently updating caches it is the responsibility of the client to atomically replace cache instance, which is provided in the request context. 
3. For updatable cache construction we need to provision memory factoring in the new cache size in addition to the existing one.

###Interfaces and APIs:

Bluewhale currently supports following types of data sources. Each has an interface called addEntry, which has to be implemented.
`addEntry` will need to have mapper to the User-specific domain POJOs. 

1. **File** (FileReaderDelegate)
2. **MySQL** (DBLoaderDelegate)
3. **Aerospike** (AerospikeLoaderDelegate)

Cache can be interacted via following methods:
* public V query(K key);
* public void init(S source) throws BlueWhaleCacheInitializationException;
* public void unsafeUpdate(S source) throws BlueWhaleCacheUpdationException;
* public Map<K,V> getAll() throws BlueWhaleCacheUpdationException;
* public int getSize();


Code samples:
-------------

### Creating reader delegate:

```java
private class Candidate {
    private int id;
    private String name;
    private String qualification;
    private int assets;
    private float prob;
}

private abstract class CSVReaderDelegate<K, V> implements FileReaderDelegate {
    
    @Override
    public <K, V> void addEntry(BufferedReader br, HashMap<K, V> entries) {

        String row = "";
        try {
            while ((row = br.readLine()) != null) {
                String[] vals = StringUtils.split(row, ",");

                // Ignore if it is header
                if (vals[0].contains("id")) {
                    continue;
                }
                // assumption here is first value is always integer id

                entries.put((K) vals[0], (V) getObjectFromValues(vals));
            }
        } catch (IOException e) {
            log.warn("Exception in reading line", e);
        }

    }
    
    protected abstract V getObjectFromValues(String[] vals);
    }
    
    private class CandidateReaderDelegate extends CSVReaderDelegate<Integer, Candidate> {
    
    @Override
    protected Candidate getObjectFromValues(String[] vals) {
        return new Candidate(Integer.valueOf(StringUtils.trim(vals[0])), vals[1],
                vals[2], Integer.valueOf(StringUtils.trim((vals[3]))), Float.valueOf(StringUtils.trim(vals[4])));
    }
}
```
	
### Cache creation and initialization:

```java

FileBlueWhaleCache fileCache = new FileBlueWhaleCache(readerDelegate,"election_candidates", 60);

try {
    fileCache.init(new File("src/test/resources/data.csv"));
} catch (BlueWhaleCacheInitializationException e) {
    Assert.fail("Failure to init cache");
}
```

### Querying cache

```java
Candidate candidate = (Candidate) fileCache.query("1");
log.info(candidate.getName());
```

Contact
------

For any features or bugs, please raise it in issues section

If anything else, get in touch with us at [opensource@zapr.in](opensource@zapr.in)