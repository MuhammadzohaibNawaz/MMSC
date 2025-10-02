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
import java.util.concurrent.ThreadLocalRandom;

/**
 * Uses Genetic Algorithm (GA) to find frequent sequential patterns.
 * Processes all .dat files in goKrimpData/original/DS/ for CTL=0,2,4,6,8,10.
 * Saves results in goKrimpData/original/DS/GA/resultsGA.csv.
 * Dynamically prioritizes pattern lengths (2, 3, 4) based on success in finding patterns.
 * @author zohaib
 */
public class GA {
    private static final int POPULATION_SIZE = 50;
    private static final int MAX_ITERATIONS = 100;
    private static final int[] CTL_VALUES = {0, 2, 4, 6, 8, 10};
    private static final int MIN_PATTERN_LENGTH = 2;
    private static final int MAX_PATTERN_LENGTH = 4;
    private static final double MUTATION_RATE = 0.3; // 30% chance per gene
    private static final int TOURNAMENT_SIZE = 3;
    
    private static List<String> sequences;
    private static List<String> originalSequences;
    private static Set<String> uniqueItems;
    private static final Random random = new Random(42);
    private static List<PatternResult> foundPatterns;
    private static String folderPath = "goKrimpData/original/DS/";
    private static String outputFolder = "goKrimpData/original/DS/GA/";
    private static String datasetName;
    private static Map<String, Integer> patternFrequencyCache;
    private static double[] patternLengthWeights = {1.0/3, 1.0/3, 1.0/3}; // Initial weights for lengths 2, 3, 4
    private static final double WEIGHT_DECREASE = 0.1; // Amount to adjust weight for failed length
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

    static class Individual {
        String[] pattern;
        int fitness;

        Individual(int size) {
            pattern = new String[size];
            fitness = 0;
        }
    }

    public static void main(String[] args) {
        // Create output directory
        new File(outputFolder).mkdirs();
        
        // Initialize CSV file for results
        String csvFilePath = outputFolder + "resultsGA.csv";
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
                        
                        PatternResult result = findPatternGA(patternSize);
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

     private static PatternResult findPatternGA(int patternSize) {
        List<String> itemsList = new ArrayList<>(uniqueItems);
        Individual[] population = new Individual[POPULATION_SIZE];
        Individual best = null;

        // Initialize population
        for (int i = 0; i < POPULATION_SIZE; i++) {
            population[i] = new Individual(patternSize);
            for (int j = 0; j < patternSize; j++) {
                population[i].pattern[j] = itemsList.get(random.nextInt(itemsList.size()));
            }
            population[i].fitness = evaluatePattern(population[i].pattern);
            if (best == null || population[i].fitness > best.fitness) {
                best = copyIndividual(population[i]);
            }
        }

        // GA main loop
        for (int iter = 0; iter < MAX_ITERATIONS; iter++) {
            Individual[] newPopulation = new Individual[POPULATION_SIZE];

            // Elitism: keep the best
            newPopulation[0] = copyIndividual(best);

            for (int i = 1; i < POPULATION_SIZE; i += 2) {
                // Selection
                Individual parent1 = tournamentSelect(population);
                Individual parent2 = tournamentSelect(population);

                // Multipoint crossover
                Individual[] children = multipointCrossover(parent1, parent2, itemsList);

                // Multipoint mutation
                multipointMutation(children[0], itemsList);
                multipointMutation(children[1], itemsList);

                // Evaluate
                children[0].fitness = evaluatePattern(children[0].pattern);
                children[1].fitness = evaluatePattern(children[1].pattern);

                newPopulation[i] = children[0];
                if (i + 1 < POPULATION_SIZE) newPopulation[i + 1] = children[1];

                // Update best
                if (children[0].fitness > best.fitness) best = copyIndividual(children[0]);
                if (children[1].fitness > best.fitness) best = copyIndividual(children[1]);
            }
            population = newPopulation;
        }

        if (best.fitness > 0 && !containsNull(best.pattern)) {
            return new PatternResult(best.pattern, best.fitness, patternSize);
        }
        return null;
    }

    private static Individual copyIndividual(Individual ind) {
        Individual copy = new Individual(ind.pattern.length);
        System.arraycopy(ind.pattern, 0, copy.pattern, 0, ind.pattern.length);
        copy.fitness = ind.fitness;
        return copy;
    }

    private static Individual tournamentSelect(Individual[] population) {
        Individual best = null;
        for (int i = 0; i < TOURNAMENT_SIZE; i++) {
            Individual candidate = population[random.nextInt(population.length)];
            if (best == null || candidate.fitness > best.fitness) best = candidate;
        }
        return best;
    }

    private static Individual[] multipointCrossover(Individual p1, Individual p2, List<String> itemsList) {
        int size = p1.pattern.length;
        Individual c1 = new Individual(size);
        Individual c2 = new Individual(size);

        // Choose two crossover points
        int point1 = ThreadLocalRandom.current().nextInt(1, size);
        int point2 = ThreadLocalRandom.current().nextInt(point1, size);

        for (int i = 0; i < size; i++) {
            if (i < point1 || i >= point2) {
                c1.pattern[i] = p1.pattern[i];
                c2.pattern[i] = p2.pattern[i];
            } else {
                c1.pattern[i] = p2.pattern[i];
                c2.pattern[i] = p1.pattern[i];
            }
        }
        return new Individual[]{c1, c2};
    }

    private static void multipointMutation(Individual ind, List<String> itemsList) {
        for (int i = 0; i < ind.pattern.length; i++) {
            if (random.nextDouble() < MUTATION_RATE) {
                ind.pattern[i] = itemsList.get(random.nextInt(itemsList.size()));
            }
        }
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

    private static int evaluatePattern(String[] pattern) {
        int count = 0;
        
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
