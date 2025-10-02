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
 * @author zohaib
 * Updated to process multiple CTL values and all files in goKrimpData/original/DS/,
 * saving results in goKrimpData/original/DS/PSO/resultsPSO.csv.
 */
public class PSO{
    private static final int SWARM_SIZE = 30;
    private static final int MAX_ITERATIONS = 100;
    private static final double C1 = 2.0; // cognitive parameter
    private static final double C2 = 2.0; // social parameter
    private static final double W = 0.7;  // inertia weight
    private static final int[] CTL_VALUES = {0, 2, 4, 6, 8, 10}; // Multiple CTL values
    private static final int MIN_PATTERN_SIZE = 2;
    private static final int MAX_PATTERN_SIZE = 4;
    
    private static List<String> sequences;
    private static List<String> originalSequences;
    private static Set<String> uniqueItems;
    private static final Random random = new Random();
    private static List<PatternResult> foundPatterns;
    private static String folderPath = "goKrimpData/original/DS/";
    private static String outputFolder = "goKrimpData/original/DS/PSO/";
    private static String datasetName;

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
            return "Size " + size + ": " + String.join(" ", pattern) + "; (Frequency: " + frequency + ")";
        }
    }

    static class Particle {
        String[] pattern;
        double[] velocity;
        String[] bestPattern;
        int bestFitness;
        
        Particle(int size) {
            pattern = new String[size];
            velocity = new double[size];
            bestPattern = new String[size];
            bestFitness = 0;
        }
    }

    public static void main(String[] args) {
        // Create output directory
        new File(outputFolder).mkdirs();
        
        // Initialize CSV file for results
        String csvFilePath = outputFolder + "resultsPSO.csv";
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
            uniqueItems = new HashSet<>();
            try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] items = line.trim().split("\\s+");
                    sequences.add(String.join(" ", items));
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

            originalSequences = new ArrayList<>(sequences);

            // Process each CTL value
            for (int CTL : CTL_VALUES) {
                long startTime = System.currentTimeMillis();
                foundPatterns = new ArrayList<>();

                // Find patterns for current CTL
                if (CTL > 0) {
                    while (foundPatterns.size() < CTL) {
                        int patternSize = random.nextInt(MAX_PATTERN_SIZE - MIN_PATTERN_SIZE + 1) + MIN_PATTERN_SIZE;
                        System.out.println("Finding pattern #" + (foundPatterns.size() + 1) + " of size " + patternSize + " for CTL=" + CTL);
                        
                        PatternResult result = findPattern(patternSize);
                        if (result != null && result.frequency > 0 && !containsNull(result.pattern)) {
                            foundPatterns.add(result);
                            removePatternFromSequences(result.pattern);
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

    private static boolean containsNull(String[] pattern) {
        for (String item : pattern) {
            if (item == null) {
                return true;
            }
        }
        return false;
    }

    private static PatternResult findPattern(int patternSize) {
        Particle[] swarm = new Particle[SWARM_SIZE];
        String[] globalBestPattern = new String[patternSize];
        int globalBestFitness = 0;

        for (int i = 0; i < SWARM_SIZE; i++) {
            swarm[i] = new Particle(patternSize);
            initializeParticle(swarm[i]);
        }

        for (int iteration = 0; iteration < MAX_ITERATIONS; iteration++) {
            for (Particle particle : swarm) {
                int currentFitness = evaluatePattern(particle.pattern);
                
                if (currentFitness > particle.bestFitness) {
                    particle.bestFitness = currentFitness;
                    System.arraycopy(particle.pattern, 0, particle.bestPattern, 0, patternSize);
                }
                
                if (currentFitness > globalBestFitness) {
                    globalBestFitness = currentFitness;
                    System.arraycopy(particle.pattern, 0, globalBestPattern, 0, patternSize);
                }
                
                updateParticle(particle, globalBestPattern);
            }
        }

        if (globalBestFitness > 0 && !containsNull(globalBestPattern)) {
            return new PatternResult(globalBestPattern, globalBestFitness, patternSize);
        }
        return null;
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
                i += pattern.length; // Skip the matched pattern
            } else {
                result.add(tokens[i]);
                i++;
            }
        }

        return String.join(" ", result);
    }

    private static void initializeParticle(Particle particle) {
        List<String> itemsList = new ArrayList<>(uniqueItems);
        for (int i = 0; i < particle.pattern.length; i++) {
            particle.pattern[i] = itemsList.get(random.nextInt(itemsList.size()));
            particle.velocity[i] = random.nextDouble() * 2 - 1;
        }
        System.arraycopy(particle.pattern, 0, particle.bestPattern, 0, particle.pattern.length);
        particle.bestFitness = evaluatePattern(particle.pattern);
    }

    private static void updateParticle(Particle particle, String[] globalBest) {
        List<String> itemsList = new ArrayList<>(uniqueItems);
        for (int i = 0; i < particle.pattern.length; i++) {
            double r1 = random.nextDouble();
            double r2 = random.nextDouble();
            particle.velocity[i] = W * particle.velocity[i] +
                                 C1 * r1 * (itemsList.indexOf(particle.bestPattern[i]) - itemsList.indexOf(particle.pattern[i])) +
                                 C2 * r2 * (itemsList.indexOf(globalBest[i]) - itemsList.indexOf(particle.pattern[i]));

            int newIndex = (int) (itemsList.indexOf(particle.pattern[i]) + particle.velocity[i]);
            newIndex = Math.max(0, Math.min(newIndex, itemsList.size() - 1));
            particle.pattern[i] = itemsList.get(newIndex);
        }
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

    private static double encodeAndSavePatterns(int CTL) {
        String encodedFilePath = outputFolder + "encoded_" + CTL + "_" + datasetName;
        String codeTableFilePath = outputFolder + "codeTable_" + CTL + "_" + datasetName + ".txt";
        double compressionRatio = 1.0; // Default if CTL=0 or no compression

        if (CTL == 0) {
            // Copy original file as encoded f 5ile (no patterns)
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
                patternToCode.put(patternStr, nextCode);
                nextCode++;
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

    public static String replacePatternTokens(String sequence, String pattern, String code) {
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
