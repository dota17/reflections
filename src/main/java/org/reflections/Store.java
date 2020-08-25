package org.reflections;

import org.reflections.scanners.Scanner;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.reflections.util.Utils.index;

/**
 * stores metadata information in multimaps
 * <p>use the different query methods (getXXX) to query the metadata
 * <p>the query methods are string based, and does not cause the class loader to define the types
 * <p>use {@link org.reflections.Reflections#getStore()} to access this store
 */
public class Store {

    private final ConcurrentHashMap<String, Map<String, Collection<typeInfo>>> storeMap;

    protected Store(Configuration configuration) {
        storeMap = new ConcurrentHashMap<>();
        for (Scanner scanner : configuration.getScanners()) {
            String index = index(scanner.getClass());
            storeMap.computeIfAbsent(index, s -> new ConcurrentHashMap<>());
        }
    }

    /** return all indices */
    public Set<String> keySet() {
        return storeMap.keySet();
    }

    /** get the multimap object for the given {@code index}, otherwise throws a {@link org.reflections.ReflectionsException} */
    private Map<String, Collection<typeInfo>> get(String index) {
        Map<String, Collection<typeInfo>> mmap = storeMap.get(index);
        if (mmap == null) {
            throw new ReflectionsException("Scanner " + index + " was not configured");
        }
        return mmap;
    }

    /** get the values stored for the given {@code index} and {@code keys} */
    public Set<typeInfo> get(Class<?> scannerClass, String key) {
        return get(index(scannerClass), Collections.singletonList(new typeInfo(key, true)));
    }

    /** get the values stored for the given {@code index} and {@code keys} */
    public Set<typeInfo> get(String index, String key) {
        return get(index, Collections.singletonList(new typeInfo(key, true)));
    }

    /** get the values stored for the given {@code index} and {@code keys} */
    public Set<typeInfo> get(Class<?> scannerClass, Collection<typeInfo> keys) {
        return get(index(scannerClass), keys);
    }

    /** get the values stored for the given {@code index} and {@code keys} */
    private Set<typeInfo> get(String index, Collection<typeInfo> keys) {
        Map<String, Collection<typeInfo>> mmap = get(index);
        Set<typeInfo> result = new LinkedHashSet<>();
        for (String key : keys.stream().map(typeInfo::getTypeName).collect(Collectors.toSet())) {
            Collection<typeInfo> values = mmap.get(key);
            if (values != null) {
                result.addAll(values);
            }
        }
        return result;
    }

    /** recursively get the values stored for the given {@code index} and {@code keys}, including keys */
    public Set<String> getAllIncluding(Class<?> scannerClass, Collection<typeInfo> keys) {
        String index = index(scannerClass);
        Map<String, Collection<typeInfo>> mmap = get(index);
        List<typeInfo> workKeys = new ArrayList<>(keys);

        Set<typeInfo> result = new HashSet<>();
        for (int i = 0; i < workKeys.size(); i++) {
            typeInfo key = workKeys.get(i);
            if (result.add(key)) {
                Collection<typeInfo> values = mmap.get(key);
                if (values != null) {
                    workKeys.addAll(values);
                }
            }
        }
        return result.stream().filter(typeInfo -> !typeInfo.isExternal).map(typeInfo::getTypeName).collect(Collectors.toSet());
    }

    /** recursively get the values stored for the given {@code index} and {@code keys}, not including keys */
    public Set<String> getAll(Class<?> scannerClass, String key) {
        return getAllIncluding(scannerClass, get(scannerClass, key));
    }

    /** recursively get the values stored for the given {@code index} and {@code keys}, not including keys */
    public Set<String> getAll(Class<?> scannerClass, Collection<typeInfo> keys) {
        return getAllIncluding(scannerClass, get(scannerClass, keys));
    }

    public Set<String> keys(String index) {
        Map<String, Collection<typeInfo>> map = storeMap.get(index);
        return map != null ? new HashSet<>(map.keySet()) : Collections.emptySet();
    }

    public Set<String> values(String index) {
        Map<String, Collection<typeInfo>> map = storeMap.get(index);
        return map != null ? map.values().stream().flatMap(Collection::stream).map(typeInfo::getTypeName).collect(Collectors.toSet()) : Collections.emptySet();
    }

    //
    public boolean put(Class<?> scannerClass, String key, typeInfo value) {
        return put(index(scannerClass), key, value);
    }

    public boolean put(String index, String key, typeInfo value) {
        return storeMap.computeIfAbsent(index, s -> new ConcurrentHashMap<>())
                .computeIfAbsent(key, s -> Collections.synchronizedList(new ArrayList<>()))
                .add(value);
    }

    void merge(Store store) {
        if (store != null) {
            for (String indexName : store.keySet()) {
                Map<String, Collection<typeInfo>> index = store.get(indexName);
                if (index != null) {
                    for (String key : index.keySet()) {
                        for (typeInfo type : index.get(key)) {
                            put(indexName, key, type);
                        }
                    }
                }
            }
        }
    }

    public static class typeInfo {
        private String typeName;

        private boolean isExternal;

        public typeInfo(String typeName, boolean isExternal) {
            this.typeName = typeName;
            this.isExternal = isExternal;
        }

        public String getTypeName() {
            return typeName;
        }

        public boolean isExternal() {
            return isExternal;
        }

    }
}
