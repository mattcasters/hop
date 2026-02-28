package org.apache.hop.execution;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.exception.HopException;

@Getter
public class DefaultExecutionSelector implements IExecutionSelector {
  public static final SimpleDateFormat START_DATE_FORMAT = new SimpleDateFormat("yyyy/MM/dd HH:mm");

  private final boolean selectingParents;
  private final boolean selectingFailed;
  private final boolean selectingRunning;
  private final boolean selectingFinished;
  private final boolean selectingWorkflows;
  private final boolean selectingPipelines;
  private final String filterText;
  private final IExecutionDateFilter startDateFilter;

  public DefaultExecutionSelector(
      boolean selectingParents,
      boolean selectingFailed,
      boolean selectingRunning,
      boolean selectingFinished,
      boolean selectingWorkflows,
      boolean selectingPipelines,
      String filterText,
      IExecutionDateFilter startDateFilter) {
    this.selectingParents = selectingParents;
    this.selectingFailed = selectingFailed;
    this.selectingRunning = selectingRunning;
    this.selectingFinished = selectingFinished;
    this.selectingWorkflows = selectingWorkflows;
    this.selectingPipelines = selectingPipelines;
    this.filterText = filterText;
    this.startDateFilter = startDateFilter;
  }

  public boolean isSelected(Execution execution, ExecutionState executionState) {
    if (selectingParents && StringUtils.isNotEmpty(executionState.getParentId())) {
      return false;
    }
    if (selectingFailed && !executionState.isFailed()) {
      return false;
    }
    if (selectingFinished
        && !executionState.getStatusDescription().toLowerCase().startsWith("finished")) {
      return false;
    }
    if (selectingRunning
        && !executionState.getStatusDescription().toLowerCase().startsWith("running")) {
      return false;
    }
    if (selectingWorkflows && !execution.getExecutionType().equals(ExecutionType.Workflow)) {
      return false;
    }
    if (selectingPipelines && !execution.getExecutionType().equals(ExecutionType.Pipeline)) {
      return false;
    }
    if (StringUtils.isNotEmpty(filterText)) {
      boolean match = execution.getName().toLowerCase().contains(filterText.toLowerCase());
      match = match || execution.getId().contains(filterText);
      if (execution.getExecutionStartDate() != null) {
        String startDateString = START_DATE_FORMAT.format(execution.getExecutionStartDate());
        match = match || startDateString.contains(filterText);
      }
      if (!match) {
        return false;
      }
    }
    if (startDateFilter != null) {
      if (!startDateFilter.isChosenDate(execution.getExecutionStartDate())) {
        return false;
      }
    }
    return true;
  }

  /**
   * This is a default implementation to make all implementations work.
   *
   * @param location The location to load from
   * @param selector The selector to use
   * @return The list of selected execution IDs.
   * @throws HopException In case something went wrong.
   */
  public static List<String> findExecutionIDs(
      IExecutionInfoLocation location, IExecutionSelector selector) throws HopException {

    List<String> selection = new ArrayList<>();
    List<String> ids = location.getExecutionIds(false, 1000);
    for (String id : ids) {
      Execution execution = location.getExecution(id);
      ExecutionState state = location.getExecutionState(id);
      if (selector.isSelected(execution, state)) {
        selection.add(id);
      }
    }
    return selection;
  }
}
