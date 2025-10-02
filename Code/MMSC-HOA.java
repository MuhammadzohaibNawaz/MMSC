package MMSC;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.*;

import org.tukaani.xz.LZMA2Options;
import org.tukaani.xz.XZOutputStream;

/**
 * Uses Hippopotamus Optimization Algorithm (HOA) to find frequent sequential patterns.
 * Processes all .dat files in goKrimpData/original/DS/ for CTL=0,2,4,6,8,10.
 * Saves results in goKrimpData/original/DS/HOA/resultsHOA.csv.
 * Dynamically prioritizes pattern lengths (2, 3, 4) based on success in finding patterns.
 * @author zohaib
 */
public class HOA {
    private static final int POPULATION_SIZE = 50;
    private static final int MAX_ITERATIONS = 100;
    private static final double FORAGING_FACTOR = 0.6;
    private static final double TERRITORY_FACTOR = 0.4;
    private static final double LEADERSHIP_FACTOR = 0.5;
    private static final int[] CTL_VALUES = {0, 2, 4, 6, 8, 10};
    private static final int MIN_PATTERN_LENGTH = 2;
    private static final int MAX_PATTERN_LENGTH = 4;
    
    private static List<String> sequences;
    private static List<String> originalSequences;
    private static Set<String> uniqueItems;
    private static final Random random = new Random(42);
    private static List<PatternResult> foundPatterns;
    private static String folderPath = "goKrimpData/original/DS/";
    private static String outputFolder = "goKrimpData/original/DS/HOA/";
    private static String datasetName;
    private static Map<String, Integer> patternFrequencyCache;
    private static double[] patternLengthWeights = {1.0/3, 1.0/3, 1.0/3}; // Initial weights for lengths 2, 3, 4
    private static final double WEIGHT_DECREASE = 0.1; // Amount to decrease weight for failed length
    private static final double MIN_WEIGHT = 0.1; // Minimum weight to ensure all lengths are considered

    static class PatternResult {
        String[] pattern;
        int frequency;
        int size;

        PatternResult(String[] pattern, int frequency, int size) {
            this.pattern = pattern;
            this.frequency = frequency;
            this.size = size;
        }

        @Override
        public String toString() {
            return "Size " + size + ": " + String.join(" ", pattern) + " (Frequency: " + frequency + ")";
        }
    }

    static class Hippopotamus {
        String[] pattern;
        int fitness;
        
        Hippopotamus(int size) {
            pattern = new String[size];
            fitness = 0;
        }
    }

    public static void main(String[] args) {
        // Create output directory
        new File(outputFolder).mkdirs();
        
        // Initialize CSV file for results
        String csvFilePath = outputFolder + "resultsHOA.csv";
        try (BufferedWriter csvWriter = new BufferedWriter(new FileWriter(csvFilePath))) {
            csvWriter.write("Dataset,CTL,CompressionRatio,ExecutionTime(ms)\n");
        } catch (IOException e) {
            System.err.println("Error initializing CSV file: " + e.getMessage());
            return;
        }

        // Get all files in the input folder
        File folder = new File(folderPath);
        File[] files = folder.listFiles((dir, name) -> name.endsWith(".dat"));
        if (files == null || files.length == 0) {
            System.err.println("No .dat files found in " + folderPath);
            return;
        }

        // Process each file
        for (File file : files) {
            datasetName = file.getName();
            String filePath = file.getAbsolutePath();
            System.out.println("\nProcessing dataset: " + datasetName);

            // Read dataset
            sequences = new ArrayList<>();
            originalSequences = new ArrayList<>();
            uniqueItems = new HashSet<>();
            patternFrequencyCache = new HashMap<>();
            try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] items = line.trim().split("\\s+");
                    String sequence = String.join(" ", items);
                    sequences.add(sequence);
                    originalSequences.add(sequence);
                    for (String item : items) {
                        if (!item.isEmpty()) {
                            uniqueItems.add(item);
                        }
                    }
                }
            } catch (IOException e) {
                System.err.println("Error reading file " + datasetName + ": " + e.getMessage());
                continue;
            }

            // Process each CTL value
            for (int CTL : CTL_VALUES) {
                long startTime = System.currentTimeMillis();
                foundPatterns = new ArrayList<>();
                patternFrequencyCache.clear();
                // Reset pattern length weights for each CTL
                patternLengthWeights = new double[]{1.0/3, 1.0/3, 1.0/3};

                // Find patterns for current CTL
                if (CTL > 0) {
                    while (foundPatterns.size() < CTL) {
                        int patternSize = selectPatternSize();
                        System.out.println("Finding pattern #" + (foundPatterns.size() + 1) + " of size " + patternSize + " for CTL=" + CTL);
                        
                        PatternResult result = findPattern(patternSize);
                        if (result != null && result.frequency > 0 && !containsNull(result.pattern)) {
                            foundPatterns.add(result);
                            removePatternFromSequences(result.pattern);
                            patternFrequencyCache.clear();
                            // Increase weight for successful pattern length
                            adjustWeights(patternSize, true);
                        } else {
                            // Decrease weight for failed pattern length
                            adjustWeights(patternSize, false);
                        }
                    }

                    System.out.println("\nAll found patterns for CTL=" + CTL + ":");
                    for (PatternResult pattern : foundPatterns) {
                        if (pattern.frequency > 0 && !containsNull(pattern.pattern)) {
                            System.out.println(pattern);
                        }
                    }
                }

                // Encode and save patterns, calculate compression ratio
                System.out.println("\nEncoding patterns and saving files for CTL=" + CTL + "...");
                double compressionRatio = encodeAndSavePatterns(CTL);
                System.out.println("Encoding complete for CTL=" + CTL + "!");

                long endTime = System.currentTimeMillis();
                long totalTime = endTime - startTime;

                // Append results to CSV
                try (BufferedWriter csvWriter = new BufferedWriter(new FileWriter(csvFilePath, true))) {
                    csvWriter.write(String.format("%s,%d,%.2f,%d\n", datasetName, CTL, compressionRatio, totalTime));
                } catch (IOException e) {
                    System.err.println("Error writing to CSV for " + datasetName + ", CTL=" + CTL + ": " + e.getMessage());
                }

                System.out.println("\nResults for CTL=" + CTL + ":");
                System.out.println("Execution time: " + totalTime + " milliseconds (" + (totalTime / 1000.0) + " seconds)");
                System.out.printf("Compression Ratio: %.2f:1\n", compressionRatio);
            }

            // Reset sequences for next file
            sequences = new ArrayList<>(originalSequences);
        }
    }

    private static int selectPatternSize() {
        double rand = random.nextDouble();
        double cumulative = 0.0;
        for (int i = 0; i < patternLengthWeights.length; i++) {
            cumulative += patternLengthWeights[i];
            if (rand <= cumulative) {
                return i + MIN_PATTERN_LENGTH; // Maps index 0->2, 1->3, 2->4
            }
        }
        return MAX_PATTERN_LENGTH; // Fallback to max length
    }

    private static void adjustWeights(int patternSize, boolean success) {
        int index = patternSize - MIN_PATTERN_LENGTH; // Maps size 2->0, 3->1, 4->2
        double totalWeight = Arrays.stream(patternLengthWeights).sum();
        
        if (success) {
            // Increase weight for successful pattern length
            patternLengthWeights[index] = Math.min(patternLengthWeights[index] + WEIGHT_DECREASE, 1.0);
        } else {
            // Decrease weight for failed pattern length
            patternLengthWeights[index] = Math.max(patternLengthWeights[index] - WEIGHT_DECREASE, MIN_WEIGHT);
        }

        // Redistribute weights to other lengths
        double remainingWeight = totalWeight - patternLengthWeights[index];
        if (remainingWeight > 0) {
            for (int i = 0; i < patternLengthWeights.length; i++) {
                if (i != index) {
                    patternLengthWeights[i] = (patternLengthWeights[i] / remainingWeight) * (totalWeight - patternLengthWeights[index]);
                }
            }
        }

        // Normalize weights to sum to 1
        double sum = Arrays.stream(patternLengthWeights).sum();
        for (int i = 0; i < patternLengthWeights.length; i++) {
            patternLengthWeights[i] = patternLengthWeights[i] / sum;
        }

        // Debug: Print updated weights
        System.out.printf("Updated pattern length weights: 2=%.3f, 3=%.3f, 4=%.3f\n", 
            patternLengthWeights[0], patternLengthWeights[1], patternLengthWeights[2]);
    }

    private static boolean containsNull(String[] pattern) {
        for (String item : pattern) {
            if (item == null) {
                return true;
            }
        }
        return false;
    }

    private static PatternResult findPattern(int patternSize) {
        Hippopotamus[] population = new Hippopotamus[POPULATION_SIZE];
        String[] globalBestPattern = new String[patternSize];
        int globalBestFitness = 0;
        Set<String> evaluatedPatterns = new HashSet<>();

        List<String> itemsList = new ArrayList<>(uniqueItems);
        for (int i = 0; i < POPULATION_SIZE; i++) {
            population[i] = new Hippopotamus(patternSize);
            initializeHippopotamus(population[i], itemsList);
            String patternKey = String.join(" ", population[i].pattern);
            evaluatedPatterns.add(patternKey);
            if (population[i].fitness > globalBestFitness) {
                globalBestFitness = population[i].fitness;
                System.arraycopy(population[i].pattern, 0, globalBestPattern, 0, patternSize);
            }
        }

        for (int iteration = 0; iteration < MAX_ITERATIONS; iteration++) {
            for (Hippopotamus hippo : population) {
                updateForagingPhase(hippo, itemsList);
                String patternKey = String.join(" ", hippo.pattern);
                if (!evaluatedPatterns.contains(patternKey)) {
                    hippo.fitness = patternFrequencyCache.computeIfAbsent(patternKey, k -> evaluatePattern(hippo.pattern));
                    evaluatedPatterns.add(patternKey);
                } else {
                    hippo.fitness = patternFrequencyCache.getOrDefault(patternKey, 0);
                }

                updateTerritoryPhase(hippo, population[random.nextInt(POPULATION_SIZE)], itemsList);
                patternKey = String.join(" ", hippo.pattern);
                if (!evaluatedPatterns.contains(patternKey)) {
                    hippo.fitness = patternFrequencyCache.computeIfAbsent(patternKey, k -> evaluatePattern(hippo.pattern));
                    evaluatedPatterns.add(patternKey);
                } else {
                    hippo.fitness = patternFrequencyCache.getOrDefault(patternKey, 0);
                }

                updateLeadershipPhase(hippo, globalBestPattern, itemsList);
                patternKey = String.join(" ", hippo.pattern);
                if (!evaluatedPatterns.contains(patternKey)) {
                    hippo.fitness = patternFrequencyCache.computeIfAbsent(patternKey, k -> evaluatePattern(hippo.pattern));
                    evaluatedPatterns.add(patternKey);
                } else {
                    hippo.fitness = patternFrequencyCache.getOrDefault(patternKey, 0);
                }

                if (hippo.fitness > globalBestFitness) {
                    globalBestFitness = hippo.fitness;
                    System.arraycopy(hippo.pattern, 0, globalBestPattern, 0, patternSize);
                }
            }
            evaluatedPatterns.clear();
        }

        String finalPatternKey = String.join(" ", globalBestPattern);
        int finalFitness = evaluatePattern(globalBestPattern); // Recompute to ensure accuracy
        patternFrequencyCache.put(finalPatternKey, finalFitness); // Update cache
        if (finalFitness > 0 && !containsNull(globalBestPattern)) {
            return new PatternResult(globalBestPattern, finalFitness, patternSize);
        }
        return null;
    }

    private static void initializeHippopotamus(Hippopotamus hippo, List<String> itemsList) {
        for (int i = 0; i < hippo.pattern.length; i++) {
            hippo.pattern[i] = itemsList.get(random.nextInt(itemsList.size()));
        }
        String patternKey = String.join(" ", hippo.pattern);
        hippo.fitness = patternFrequencyCache.computeIfAbsent(patternKey, k -> evaluatePattern(hippo.pattern));
    }

    private static void updateForagingPhase(Hippopotamus hippo, List<String> itemsList) {
        String[] foodPosition = new String[hippo.pattern.length];
        for (int i = 0; i < hippo.pattern.length; i++) {
            foodPosition[i] = itemsList.get(random.nextInt(itemsList.size()));
        }
        for (int i = 0; i < hippo.pattern.length; i++) {
            int currentIdx = itemsList.indexOf(hippo.pattern[i]);
            int foodIdx = itemsList.indexOf(foodPosition[i]);
            double r = random.nextDouble();
            int newIdx = (int) (currentIdx + r * FORAGING_FACTOR * (foodIdx - currentIdx));
            newIdx = Math.max(0, Math.min(newIdx, itemsList.size() - 1));
            hippo.pattern[i] = itemsList.get(newIdx);
        }
    }

    private static void updateTerritoryPhase(Hippopotamus hippo, Hippopotamus neighbor, List<String> itemsList) {
        for (int i = 0; i < hippo.pattern.length; i++) {
            int currentIdx = itemsList.indexOf(hippo.pattern[i]);
            int neighborIdx = itemsList.indexOf(neighbor.pattern[i]);
            double r = random.nextDouble();
            int newIdx = (int) (currentIdx + r * TERRITORY_FACTOR * (neighborIdx - currentIdx));
            newIdx = Math.max(0, Math.min(newIdx, itemsList.size() - 1));
            hippo.pattern[i] = itemsList.get(newIdx);
        }
    }

    private static void updateLeadershipPhase(Hippopotamus hippo, String[] globalBest, List<String> itemsList) {
        for (int i = 0; i < hippo.pattern.length; i++) {
            int currentIdx = itemsList.indexOf(hippo.pattern[i]);
            int bestIdx = itemsList.indexOf(globalBest[i]);
            double r = random.nextDouble();
            int newIdx = (int) (currentIdx + r * LEADERSHIP_FACTOR * (bestIdx - currentIdx));
            newIdx = Math.max(0, Math.min(newIdx, itemsList.size() - 1));
            hippo.pattern[i] = itemsList.get(newIdx);
        }
    }

    private static int evaluatePattern(String[] pattern) {
        int count = 0;
        String patternStr = String.join(" ", pattern);
        
        for (String sequence : sequences) {
            String[] tokens = sequence.trim().split("\\s+");
            for (int i = 0; i <= tokens.length - pattern.length; i++) {
                if (isContiguousMatch(tokens, i, pattern)) {
                    count++;
                    i += pattern.length - 1; // Skip to avoid overlapping matches
                }
            }
        }
        
        return count;
    }

    private static boolean isContiguousMatch(String[] tokens, int start, String[] pattern) {
        if (start + pattern.length > tokens.length) {
            return false;
        }
        for (int i = 0; i < pattern.length; i++) {
            if (!tokens[start + i].equals(pattern[i])) {
                return false;
            }
        }
        return true;
    }

    private static void removePatternFromSequences(String[] pattern) {
        List<String> newSequences = new ArrayList<>();
        
        for (String sequence : sequences) {
            String[] tokens = sequence.trim().split("\\s+");
            String newSequence = replacePatternWithEmpty(tokens, pattern);
            if (!newSequence.trim().isEmpty()) {
                newSequences.add(newSequence);
            }
        }
        
        sequences = newSequences;
    }

    private static String replacePatternWithEmpty(String[] tokens, String[] pattern) {
        List<String> result = new ArrayList<>();
        int i = 0;

        while (i < tokens.length) {
            if (i <= tokens.length - pattern.length && isContiguousMatch(tokens, i, pattern)) {
                i += pattern.length;
            } else {
                result.add(tokens[i]);
                i++;
            }
        }

        return String.join(" ", result);
    }

    private static double encodeAndSavePatterns(int CTL) {
        String encodedFilePath = outputFolder + "encoded_" + CTL + "_" + datasetName;
        String codeTableFilePath = outputFolder + "codeTable_" + CTL + "_" + datasetName + ".txt";
        double compressionRatio = 1.0; // Default if CTL=0 or no compression

        if (CTL == 0) {
            // Copy original file as encoded file (no patterns)
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(encodedFilePath))) {
                for (String sequence : originalSequences) {
                    writer.write(sequence + "\n");
                }
            } catch (IOException e) {
                System.err.println("Error writing encoded file for CTL=0: " + e.getMessage());
            }
            // Create empty code table
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(codeTableFilePath))) {
                writer.write("");
            } catch (IOException e) {
                System.err.println("Error writing code table for CTL=0: " + e.getMessage());
            }
        } else {
            // Encode with patterns
            int maxNumber = 0;
            for (String sequence : originalSequences) {
                String[] items = sequence.split("\\s+");
                for (String item : items) {
                    try {
                        int num = Integer.parseInt(item);
                        maxNumber = Math.max(maxNumber, num);
                    } catch (NumberFormatException e) {
                        // Not a number, skip
                    }
                }
            }
            
            int nextCode = maxNumber + 1;
            Map<String, Integer> patternToCode = new HashMap<>();
            
            for (PatternResult pattern : foundPatterns) {
                String patternStr = String.join(" ", pattern.pattern);
                while (uniqueItems.contains(String.valueOf(nextCode))) {
                    nextCode++;
                }
                patternToCode.put(patternStr, nextCode++);
            }
            
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(codeTableFilePath))) {
                for (Map.Entry<String, Integer> entry : patternToCode.entrySet()) {
                    writer.write(entry.getValue() + ":" + entry.getKey() + "\n");
                }
            } catch (IOException e) {
                System.err.println("Error writing code table for " + datasetName + ", CTL=" + CTL + ": " + e.getMessage());
            }
            
            List<String> encodedSequences = new ArrayList<>();
            for (String sequence : originalSequences) {
                String encodedSequence = sequence;
                List<Map.Entry<String, Integer>> sortedPatterns = new ArrayList<>(patternToCode.entrySet());
                sortedPatterns.sort((a, b) -> b.getKey().length() - a.getKey().length());
                
                for (Map.Entry<String, Integer> entry : sortedPatterns) {
                    encodedSequence = replacePatternTokens(encodedSequence, entry.getKey(), entry.getValue().toString());
                }
                encodedSequences.add(encodedSequence);
            }
            
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(encodedFilePath))) {
                for (String sequence : encodedSequences) {
                    writer.write(sequence + "\n");
                }
            } catch (IOException e) {
                System.err.println("Error writing encoded file for " + datasetName + ", CTL=" + CTL + ": " + e.getMessage());
            }
        }

        // Compress the encoded file
        try {
            LZMA2Options options = new LZMA2Options();
            options.setPreset(9);
            String compressedFilePath = outputFolder + "zipencoded_" + CTL + "_" + datasetName;

            try (FileInputStream in = new FileInputStream(encodedFilePath);
                 FileOutputStream out = new FileOutputStream(compressedFilePath);
                 XZOutputStream xzOut = new XZOutputStream(out, options)) {
                
                byte[] buffer = new byte[8192];
                int len;
                while ((len = in.read(buffer)) != -1) {
                    xzOut.write(buffer, 0, len);
                }
            }
            System.out.println("XZ compression completed successfully for CTL=" + CTL + "!");

            File originalFile = new File(folderPath + datasetName);
            File compressedFile = new File(compressedFilePath);
            File codeTableFile = new File(codeTableFilePath);
            
            long originalSize = originalFile.length();
            long compressedSize = compressedFile.length() + (CTL == 0 ? 0 : codeTableFile.length());
            compressionRatio = (compressedSize == 0) ? 1.0 : (double) originalSize / compressedSize;
            
        } catch (IOException e) {
            System.err.println("Error during XZ compression for " + datasetName + ", CTL=" + CTL + ": " + e.getMessage());
        }

        return compressionRatio;
    }

    private static String replacePatternTokens(String sequence, String pattern, String code) {
        String[] tokens = sequence.trim().split("\\s+");
        String[] patternTokens = pattern.trim().split("\\s+");
        List<String> result = new ArrayList<>();
        int i = 0;

        while (i < tokens.length) {
            if (i <= tokens.length - patternTokens.length && isContiguousMatch(tokens, i, patternTokens)) {
                result.add(code);
                i += patternTokens.length;
            } else {
                result.add(tokens[i]);
                i++;
            }
        }

        return String.join(" ", result);
    }
}
