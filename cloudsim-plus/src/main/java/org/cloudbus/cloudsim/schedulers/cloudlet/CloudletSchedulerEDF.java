package org.cloudbus.cloudsim.schedulers.cloudlet;

import org.cloudbus.cloudsim.cloudlets.Cloudlet;
import org.cloudbus.cloudsim.cloudlets.CloudletExecution;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.resources.Pe;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;


public class CloudletSchedulerEDF extends CloudletSchedulerAbstract{

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
                    return Double.compare(lhs.getDeadline(), rhs.getDeadline());
                }
            };
            sortCloudletWaitingList(comparator);
        }

        return 0.0;
    }

    private boolean canExecuteCloudlet(final CloudletExecution cle){
        return cle.getCloudlet().getStatus().ordinal() < Cloudlet.Status.FROZEN.ordinal()
            && canExecuteCloudletInternal(cle);
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
                return lhs.getDeadline() > rhs.getDeadline() ?  1 : lhs.getDeadline() < rhs.getDeadline() ? -1 : 0;
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

    private double movePausedCloudletToExecListOrWaitingList(CloudletExecution c) {
        getCloudletPausedList().remove(c);

        // it can go to the exec list
        if (isThereEnoughFreePesForCloudlet(c)) {
            return movePausedCloudletToExecList(c);
        }

        addCloudletToWaitingList(c);
        return 0.0;
    }

    private double movePausedCloudletToExecList(CloudletExecution c) {
        addCloudletToExecList(c);
        return cloudletEstimatedFinishTime(c, getVm().getSimulation().clock());
    }

    @Override
    protected boolean canExecuteCloudletInternal(final CloudletExecution cloudlet) {
        return isThereEnoughFreePesForCloudlet(cloudlet);
    }

}

