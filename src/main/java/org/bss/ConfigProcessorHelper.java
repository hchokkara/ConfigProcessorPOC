package org.bss;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class ConfigProcessorHelper {

    public void processConfigurations(ObjectNode sourceConfig, ObjectNode destinationConfig, JsonNode configDetails) {
        ArrayNode nodesToProcess = (ArrayNode) configDetails.get("nodesToProcess");

        for (JsonNode nodeToProcess : nodesToProcess) {
            String nodeName = nodeToProcess.get("nodeName").asText();
            String action = nodeToProcess.get("action").asText();
            JsonNode sourceNode = sourceConfig.get(nodeName);

            switch (action.toLowerCase()) {
                case "copy":
                case "add":
                    processCopyOrAdd(sourceConfig, destinationConfig, sourceNode, nodeName, nodeToProcess, action);
                    break;
                case "delete":
                    processDelete(destinationConfig, nodeName, nodeToProcess);
                    break;
                default:
                    System.out.println("Unsupported action: " + action);
            }
        }
    }

//    private static void processCopyOrAdd(ObjectNode sourceConfig, ObjectNode destinationConfig, JsonNode sourceNode, String nodeName, JsonNode nodeToProcess) {
//        if (sourceNode == null || !sourceNode.isArray()) {
//            System.out.println("Source node is null or not an array: " + sourceNode);
//            return;
//        }
//
//        ArrayNode destinationArray = destinationConfig.withArray(nodeName);
//        Map<String, JsonNode> keyMap = new HashMap<>();
//        JsonNode transform = nodeToProcess.get("transform");
//        JsonNode excludeFields = nodeToProcess.get("excludeFields");
//
//        // Build transformation map
//        if (transform != null) {
//            String sourceNodeName = transform.get("sourceNodeName").asText();
//            String keyField = transform.get("keyField").asText();
//            String valueField = transform.get("valueField").asText();
//
//            JsonNode sourceNodeForMap = sourceConfig.get(sourceNodeName);
//            if (sourceNodeForMap != null && sourceNodeForMap.isArray()) {
//                for (JsonNode field : sourceNodeForMap) {
//                    if (field.has(keyField) && field.has(valueField)) {
//                        String key = field.get(keyField).asText();
//                        JsonNode value = field.get(valueField);
//                        keyMap.put(key, value);
//                    }
//                }
//            } else {
//                System.out.println("Source node for transformation is null or not an array: " + sourceNodeName);
//            }
//        }
//
//        // Process source array
//        for (JsonNode sourceItem : sourceNode) {
//            if (!(sourceItem instanceof ObjectNode)) {
//                continue;
//            }
//
//            ObjectNode copiedNode = ((ObjectNode) sourceItem).deepCopy();
//
//            // Exclude specified fields
//            if (excludeFields != null && excludeFields.isArray()) {
//                for (JsonNode field : excludeFields) {
//                    copiedNode.remove(field.asText());
//                }
//            }
//
//            // Apply transformation
//            if (transform != null) {
//                String targetField = transform.get("targetField").asText();
//                String addField = transform.get("addField").asText();
//                if (keyMap.containsKey(copiedNode.get(targetField).asText())) {
//                    copiedNode.put(addField, keyMap.get(copiedNode.get(targetField).asText()).asInt());
//                }
//
//                // remove or replace existing node
//                for (int i = destinationArray.size() - 1; i >= 0; i--) {
//                    JsonNode node = destinationArray.get(i);
//                    if (node.has(targetField)) {
//                        if (node.get(targetField).asText().equals(copiedNode.get(targetField).asText())) {
//                            destinationArray.remove(i);
//                        }
//                    }
//                }
//            }
//            destinationArray.add(copiedNode);
//        }
//
//        System.out.println("Final destination array after processing: " + destinationArray);
//    }

    private void processCopyOrAdd(ObjectNode sourceConfig, ObjectNode destinationConfig, JsonNode sourceNode,
                                         String nodeName, JsonNode nodeToProcess, String action) {
        if (sourceNode == null) {
            System.out.println("Source node is null for: " + nodeName);
            return;
        }

        // Extract the list of fields to retain from the input JSON
        List<String> fieldsToRetain = new ArrayList<>();
        if (nodeToProcess.has("retain") && nodeToProcess.get("retain").isArray()) {
            for (JsonNode fieldNode : nodeToProcess.get("retain")) {
                fieldsToRetain.add(fieldNode.asText());
            }
        }

        // Handle Array Nodes
        if (sourceNode.isArray()) {
            ArrayNode destinationArray = destinationConfig.withArray(nodeName);
            Map<String, JsonNode> keyMap = buildKeyMap(sourceConfig, nodeToProcess.get("transform"));
            JsonNode filter = nodeToProcess.get("filter");
            List<String> filterValues = getFilterValues(filter);

            for (JsonNode sourceItem : sourceNode) {
                if (!(sourceItem instanceof ObjectNode) ||
                        (!filterValues.isEmpty() &&
                                !filterValues.contains(sourceItem.get(filter.get("field").asText()).asText())))
                    continue;

                ObjectNode copiedNode = sourceItem.deepCopy();

                excludeFields(copiedNode, nodeToProcess.get("exclude"));
                applyTransformations(copiedNode, keyMap, nodeToProcess.get("transform"));

                if (action.equalsIgnoreCase("add")) {
                    copiedNode.put("addedField", "newValue"); // Example for adding new field
                } else {
                    if (nodeToProcess.has("transform")) {
                        String targetField = nodeToProcess.get("transform").get("targetField").asText();
                        // remove existing node
                        for (int i = destinationArray.size() - 1; i >= 0; i--) {
                            JsonNode node = destinationArray.get(i);
                            if (node.has(targetField)) {
                                if (node.get(targetField).asText().equals(copiedNode.get(targetField).asText())) {
                                    retainFields(copiedNode, (ObjectNode) destinationArray.remove(i), fieldsToRetain);
                                }
                            }
                        }
                    }
                }
                destinationArray.add(copiedNode);
            }
        } else if (sourceNode.isObject()) { // Handle Object Nodes
            ObjectNode destinationObject = destinationConfig.with(nodeName);
            ObjectNode copiedNode = sourceNode.deepCopy();

            excludeFields(copiedNode, nodeToProcess.get("exclude"));
            applyTransformations(copiedNode, buildKeyMap(sourceConfig, nodeToProcess.get("transform")), nodeToProcess.get("transform"));
            retainFields(copiedNode, destinationObject, fieldsToRetain);

            if (action.equalsIgnoreCase("add")) {
                copiedNode.put("addedField", "newValue"); // Example for adding new field
            }

            destinationObject.setAll(copiedNode);
        }
        // Handle Scalar Nodes
        else if (sourceNode.isValueNode()) {
            destinationConfig.put(nodeName, sourceNode.asText());
        }
    }

    private void processDelete(ObjectNode destinationConfig, String nodeName, JsonNode nodeToProcess) {
        JsonNode destinationNode = destinationConfig.get(nodeName);
        if (destinationNode == null) {
            System.out.println("Destination node is null for deletion: " + nodeName);
            return;
        }

        if (destinationNode.isArray()) {
            ArrayNode destinationArray = (ArrayNode) destinationNode;
            JsonNode filter = nodeToProcess.get("filter");

            if (filter == null) {
                destinationConfig.remove(nodeName); // Remove entire array if no filter
            } else {
                String field = filter.get("field").asText();
                List<String> filterValues = getFilterValues(filter);

                for (int i = destinationArray.size() - 1; i >= 0; i--) {
                    JsonNode node = destinationArray.get(i);
                    if (node.has(field)) {
                        String value = node.get(field).asText();
                        if (filterValues.contains(value)) {
                            destinationArray.remove(i);
                        }
                    }
                }
            }
        } else if (destinationNode.isObject()) {
            destinationConfig.remove(nodeName); // Remove entire object node
        } else {
            destinationConfig.remove(nodeName); // Remove scalar node
        }
    }

    private List<String> getFilterValues(JsonNode filter) {
        List<String> valuesList = new ArrayList<>();
        if (filter != null && filter.isObject()) {
            JsonNode valuesNode = filter.get("values");
            if (valuesNode != null && valuesNode.isArray()) {
                for (JsonNode value : valuesNode) {
                    if (value.isTextual()) {
                        valuesList.add(value.asText());
                    }
                }
            }
        }
        return valuesList;
    }

    private Map<String, JsonNode> buildKeyMap(ObjectNode sourceConfig, JsonNode transform) {
        Map<String, JsonNode> keyMap = new HashMap<>();
        if (transform != null) {
            String sourceNodeName = transform.get("sourceNodeName").asText();
            String keyField = transform.get("keyField").asText();
            String valueField = transform.get("valueField").asText();
            JsonNode sourceNodeForMap = sourceConfig.get(sourceNodeName);

            if (sourceNodeForMap != null && sourceNodeForMap.isArray()) {
                for (JsonNode field : sourceNodeForMap) {
                    if (field.has(keyField) && field.has(valueField)) {
                        keyMap.put(field.get(keyField).asText(), field.get(valueField));
                    }
                }
            }
        }
        return keyMap;
    }

    private void excludeFields(ObjectNode node, JsonNode excludeFields) {
        if (excludeFields != null && excludeFields.isArray()) {
            for (JsonNode field : excludeFields) {
                node.remove(field.asText());
            }
        }
    }

    private void retainFields(ObjectNode newNode, ObjectNode destinationNode, List<String> fieldsToRetain) {
        for (String fieldToRetain : fieldsToRetain) {
            if (destinationNode.has(fieldToRetain)) {
                newNode.put(fieldToRetain, destinationNode.get(fieldToRetain));
            }
        }
    }

    private void applyTransformations(ObjectNode node, Map<String, JsonNode> keyMap, JsonNode transform) {
        if (transform != null) {
            String targetField = transform.get("targetField").asText();
            String addField = transform.get("addField").asText();
            if (node.has(targetField) && keyMap.containsKey(node.get(targetField).asText())) {
                node.set(addField, keyMap.get(node.get(targetField).asText()));
            }
        }
    }
}
