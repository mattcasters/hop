package org.apache.hop.execution;

public interface IExecutionSelector {
  boolean isSelectingParents();

  boolean isSelectingFailed();

  boolean isSelectingRunning();

  boolean isSelectingFinished();

  boolean isSelectingPipelines();

  boolean isSelectingWorkflows();

  String getFilterText();

  IExecutionDateFilter getStartDateFilter();

  boolean isSelected(Execution execution, ExecutionState executionState);
}
