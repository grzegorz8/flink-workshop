package com.xebia.flink.workshop.stateprocessorapi;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.runtime.checkpoint.Checkpoints;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.state.api.OperatorIdentifier;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
class SavepointInspector {

    private final Map<OperatorID, String> nameByOperatorId;

    SavepointInspector(String... uids) {
        nameByOperatorId = new HashMap<>();
        for (String uid : uids) {
            nameByOperatorId.put(OperatorIdentifier.forUid(uid).getOperatorId(), uid);
        }
    }

    void inspect(String label, String savepointPath) throws Exception {
        Path savepointFsPath = savepointPath.startsWith("file:")
                ? Path.of(URI.create(savepointPath))
                : Path.of(savepointPath);
        Path metadataFile = savepointFsPath.resolve("_metadata");
        if (!Files.exists(metadataFile)) {
            metadataFile = savepointFsPath;
        }
        try (DataInputStream in = new DataInputStream(new BufferedInputStream(Files.newInputStream(metadataFile)))) {
            CheckpointMetadata metadata = Checkpoints.loadCheckpointMetadata(in, getClass().getClassLoader(), savepointPath);

            String fmt = "  %-28s %5s %8s %10s %13s %10s %11s %8s %10s%n";
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("%n[%s] checkpointId=%d%n", label, metadata.getCheckpointId()));
            sb.append(String.format(fmt, "uid", "par", "maxPar", "subtasks", "managedKeyed", "rawKeyed", "managedOp", "rawOp", "size"));
            sb.append("  ").append("-".repeat(108)).append("%n".formatted());

            List<OperatorState> operators = metadata.getOperatorStates().stream()
                    .sorted(Comparator.comparing(op -> nameByOperatorId.getOrDefault(op.getOperatorID(), op.getOperatorID().toString())))
                    .toList();

            for (OperatorState op : operators) {
                String name = nameByOperatorId.getOrDefault(op.getOperatorID(), op.getOperatorID().toString().substring(0, 8));
                int managedKeyed = 0, rawKeyed = 0, managedOp = 0, rawOp = 0;
                long totalBytes = 0;
                for (OperatorSubtaskState sub : op.getSubtaskStates().values()) {
                    managedKeyed += sub.getManagedKeyedState().size();
                    rawKeyed += sub.getRawKeyedState().size();
                    managedOp += sub.getManagedOperatorState().size();
                    rawOp += sub.getRawOperatorState().size();
                    totalBytes += sub.getManagedKeyedState().getStateSize()
                            + sub.getRawKeyedState().getStateSize()
                            + sub.getManagedOperatorState().getStateSize()
                            + sub.getRawOperatorState().getStateSize();
                }
                sb.append(String.format(fmt, name, op.getParallelism(), op.getMaxParallelism(),
                        op.getSubtaskStates().size(), managedKeyed, rawKeyed, managedOp, rawOp, formatSize(totalBytes)));
            }
            log.info(sb.toString());
        }
    }

    private static String formatSize(long bytes) {
        if (bytes < 1024) return bytes + " B";
        if (bytes < 1024 * 1024) return String.format("%.1f KB", bytes / 1024.0);
        return String.format("%.1f MB", bytes / (1024.0 * 1024));
    }
}