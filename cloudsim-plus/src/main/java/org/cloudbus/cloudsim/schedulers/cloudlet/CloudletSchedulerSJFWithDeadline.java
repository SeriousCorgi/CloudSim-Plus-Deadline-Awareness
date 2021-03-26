package org.cloudbus.cloudsim.schedulers.cloudlet;

import org.cloudbus.cloudsim.cloudlets.Cloudlet;
import org.cloudbus.cloudsim.cloudlets.CloudletExecution;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.resources.Pe;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;


public class CloudletSchedulerSJFWithDeadline extends CloudletSchedulerAbstract{

    @Override
    protected double cloudletSubmitInternal(final CloudletExecution cle, final double fileTransferTime) {
        if (!isSchedulable(cle)) {
            cle.setStatus(Cloudlet.Status.FAILED);
            cloudletFail(cle);
        } else if (canExecuteCloudlet(cle)) {
            cle.setStatus(Cloudlet.Status.INEXEC);
            cle.setFileTransferTime(fileTransferTime);
            addCloudletToExecList(cle);
            return fileTransferTime+ Math.abs(cle.getCloudletLength()/getPeCapacity())  ;
        } else {

            // No enough free PEs, then add Cloudlet to the waiting queue
            addCloudletToWaitingList(cle);


            Comparator<CloudletExecution> comparator = new Comparator<CloudletExecution>() {
                @Override
                public int compare(CloudletExecution lhs, CloudletExecution rhs) {
                    // -1 - less than, 1 - greater than, 0 - equal, all inversed for descending
                    return lhs.getCloudletLength() > rhs.getCloudletLength() ? 1 : (lhs.getCloudletLength() < rhs.getCloudletLength()) ? -1 : 0;
                }
            };
            sortCloudletWaitingList(comparator);
        }

        return 0.0;
    }

    private boolean canExecuteCloudlet(final CloudletExecution cle){
        return cle.getCloudlet().getStatus().ordinal() < Cloudlet.Status.FROZEN.ordinal() && canExecuteCloudletInternal(cle);
    }

    private Double getPeCapacity() {
        return getCurrentMipsShare().stream().findFirst().orElse(0.0);
    }


    private boolean isSchedulable(CloudletExecution cle) {
        double capacity = 0.0;
        int cpus = 0;
        for (Double mips : getCurrentMipsShare()) {
            capacity += mips;
            if (mips > 0) {
                cpus++;
            }
        }

        capacity /= cpus;
        double nextEvent = Double.MAX_VALUE;
        double totalexpected = CloudSim.NULL.clock();
        for (CloudletExecution ce : getCloudletExecList()) {
            double remainingLength = ce.getRemainingCloudletLength();
            double estimatedFinishTime = (remainingLength / capacity);
            if (estimatedFinishTime < CloudSim.NULL.getMinTimeBetweenEvents()) {
                estimatedFinishTime = CloudSim.NULL.getMinTimeBetweenEvents();
            }
            if(ce.getCloudletArrivalTime() > totalexpected) {
                totalexpected = ce.getCloudletArrivalTime();
            }
            if (estimatedFinishTime < nextEvent) {
                totalexpected += estimatedFinishTime;
            }
        }

        List<CloudletExecution> waitinglist = new ArrayList<>();
        waitinglist.addAll(getCloudletWaitingList());

        waitinglist.add(cle);
        Collections.sort(waitinglist, new Comparator<CloudletExecution>() {
            @Override
            public int compare(CloudletExecution lhs, CloudletExecution rhs) {
                // -1 - less than, 1 - greater than, 0 - equal, all inversed for descending
                return lhs.getCloudletLength() > rhs.getCloudletLength() ?  1 : lhs.getCloudletLength() < rhs.getCloudletLength() ? -1 : 0;
            }
        });

        for(CloudletExecution ce : waitinglist) {
            double remainingLength = ce.getRemainingCloudletLength();
            if(ce.getCloudletId() == cle.getCloudletId()) {
                remainingLength = cle.getCloudletLength();
            }else if(remainingLength == 0){
                remainingLength = ce.getCloudletLength();
            }

            double estimatedFinishTime = (remainingLength / capacity);
            if (estimatedFinishTime < CloudSim.NULL.getMinTimeBetweenEvents()) {
                estimatedFinishTime = CloudSim.NULL.getMinTimeBetweenEvents();
            }
            if(ce.getCloudletArrivalTime() > totalexpected) {
                totalexpected = ce.getCloudletArrivalTime();
            }
            if (estimatedFinishTime < nextEvent) {
                totalexpected += estimatedFinishTime;
            }
            if (totalexpected > ce.getDeadline()) {
                return false;
            }
        }

        return true;
    }

    private void cloudletFail(final CloudletExecution cle) {
        cle.setStatus(Cloudlet.Status.FAILED);
//        getCloudletFinishedList().add(cle);
    }


    /**
     * This code below implements CloudletSchedulerSpaceShared
     * Only one cloudlet can be execute at a time in each VM
     */


    @Override
    public double cloudletResume(Cloudlet cloudlet) {
        return findCloudletInList(cloudlet, getCloudletPausedList())
            .map(this::movePausedCloudletToExecListOrWaitingList)
            .orElse(0.0);
    }

    /**
     * Moves a Cloudlet that is being resumed to the exec or waiting List.
     *
     * @param c the resumed Cloudlet to move
     * @return the time the cloudlet is expected to finish or zero if it was moved to the waiting list
     */
    private double movePausedCloudletToExecListOrWaitingList(CloudletExecution c) {
        getCloudletPausedList().remove(c);

        // it can go to the exec list
        if (isThereEnoughFreePesForCloudlet(c)) {
            return movePausedCloudletToExecList(c);
        }

        // No enough free PEs: go to the waiting queue
        /*
         * A resumed cloudlet is not immediately added to the execution list.
         * It is queued so that the next time the scheduler process VM execution,
         * the cloudlet may have the opportunity to run.
         * It goes to the end of the waiting list because other cloudlets
         * could be waiting longer and have priority to execute.
         */
        addCloudletToWaitingList(c);
        return 0.0;
    }

    /**
     * Moves a paused cloudlet to the execution list.
     *
     * @param c the cloudlet to be moved
     * @return the time the cloudlet is expected to finish
     */
    private double movePausedCloudletToExecList(CloudletExecution c) {
        addCloudletToExecList(c);
        return cloudletEstimatedFinishTime(c, getVm().getSimulation().clock());
    }

    /**
     * The space-shared scheduler <b>does not</b> share the CPU time between
     * executing cloudlets. Each CPU ({@link Pe}) is used by another Cloudlet
     * just when the previous Cloudlet using it has finished executing
     * completely. By this way, if there are more Cloudlets than PEs, some
     * Cloudlet will not be allowed to start executing immediately.
     *
     * @param cloudlet {@inheritDoc}
     * @return {@inheritDoc}
     */
    @Override
    protected boolean canExecuteCloudletInternal(final CloudletExecution cloudlet) {
        return isThereEnoughFreePesForCloudlet(cloudlet);
    }

}
