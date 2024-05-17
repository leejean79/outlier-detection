package Algorithms.iforest.utlis;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public class MyIdentifiable {
    public static void main(String[] args) {
        String randomUid = generateForestId("IForest");

        System.out.println(randomUid);
    }

    public static String generateForestId(String prefix) {
//        ThreadLocalRandom random = ThreadLocalRandom.current();
        UUID uuid = UUID.randomUUID();
        return prefix + "_" + uuid.toString();
    }
}
