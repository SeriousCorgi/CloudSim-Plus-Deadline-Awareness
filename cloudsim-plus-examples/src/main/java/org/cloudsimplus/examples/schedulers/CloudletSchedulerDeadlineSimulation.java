package org.cloudsimplus.examples.schedulers;


    import org.cloudbus.cloudsim.allocationpolicies.VmAllocationPolicySimple;
    import org.cloudbus.cloudsim.brokers.DatacenterBroker;
    import org.cloudbus.cloudsim.brokers.DatacenterBrokerSimple;
    import org.cloudbus.cloudsim.cloudlets.Cloudlet;
    import org.cloudbus.cloudsim.cloudlets.CloudletSimple;
    import org.cloudbus.cloudsim.core.CloudSim;
    import org.cloudbus.cloudsim.datacenters.Datacenter;
    import org.cloudbus.cloudsim.datacenters.DatacenterSimple;
    import org.cloudbus.cloudsim.hosts.Host;
    import org.cloudbus.cloudsim.hosts.HostSimple;
    import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
    import org.cloudbus.cloudsim.provisioners.ResourceProvisioner;
    import org.cloudbus.cloudsim.provisioners.ResourceProvisionerSimple;
    import org.cloudbus.cloudsim.resources.Pe;
    import org.cloudbus.cloudsim.resources.PeSimple;
    import org.cloudbus.cloudsim.schedulers.cloudlet.CloudletSchedulerEDF;
    import org.cloudbus.cloudsim.schedulers.cloudlet.CloudletSchedulerFCFSWithDeadline;
    import org.cloudbus.cloudsim.schedulers.cloudlet.CloudletSchedulerSJFWithDeadline;
    import org.cloudbus.cloudsim.schedulers.vm.VmScheduler;
    import org.cloudbus.cloudsim.schedulers.vm.VmSchedulerTimeShared;
    import org.cloudbus.cloudsim.util.SwfWorkloadFileReader;
    import org.cloudbus.cloudsim.utilizationmodels.UtilizationModel;
    import org.cloudbus.cloudsim.utilizationmodels.UtilizationModelFull;
    import org.cloudbus.cloudsim.vms.Vm;
    import org.cloudbus.cloudsim.vms.VmSimple;
    import org.cloudsimplus.builders.tables.CloudletsTableBuilder;

    import java.io.BufferedReader;
    import java.io.FileReader;
    import java.io.IOException;
    import java.util.ArrayList;
    import java.util.List;


public class CloudletSchedulerDeadlineSimulation {

    private static final String REAL_WORKLOAD_FILENAME = "workload/swf/KTH-SP2-1996-2.1-cln.swf.gz";
    private int maximumNumberOfCloudletsToCreateFromTheWorkloadFile = -1;


    private static final String SYN_WORKLOAD_100 = "100workloads.txt";
    private static final String SYN_WORKLOAD_200 = "200workloads.txt";
    private static final String SYN_WORKLOAD_300 = "300workloads.txt";
    private static final String SYN_WORKLOAD_400 = "400workloads.txt";
    private static final String SYN_WORKLOAD_500 = "500workloads.txt";

    private static final int HOSTS = 10000;
    private static final int HOST_PES = 12;

    private static final int VMS = 1;
    private static final int VM_PES = 4;
    private static final int  VM_MIPS = 5000;
    private static final long VM_SIZE = 2000;
    private static final int  VM_RAM = 1000;
    private static final long VM_BW = 50000;

    private static final int CLOUDLETS = 4;
    private static final int CLOUDLET_PES = 4;
    private static final int CLOUDLET_LENGTH = 10000;

    private final CloudSim simulation;
    private DatacenterBroker broker0;
    private List<Vm> vmList;
    private List<Cloudlet> cloudletList;
    private Datacenter datacenter0;

    public static void main(String[] args) {
        new CloudletSchedulerDeadlineSimulation();
    }

    private CloudletSchedulerDeadlineSimulation() {

        simulation = new CloudSim();
        datacenter0 = createDatacenter();

        //Creates a broker that is a software acting on behalf a cloud customer to manage his/her VMs and Cloudlets
        broker0 = new DatacenterBrokerSimple(simulation);

        vmList = createVms();
        cloudletList = createCloudlets();
//        cloudletList = readCloudletsFromFile(SYN_WORKLOAD_500);
//        createCloudletsFromWorkloadFile(broker0);
        broker0.submitVmList(vmList);
        broker0.submitCloudletList(cloudletList);

        simulation.start();

        final List<Cloudlet> finishedCloudlets = broker0.getCloudletFinishedList();
        new CloudletsTableBuilder(finishedCloudlets).build();
        System.out.println("Succesful Task Count: " + finishedCloudlets.size());

        double totalWaitingTime = 0;
        for (Cloudlet c : finishedCloudlets) {
            totalWaitingTime += c.getWaitingTime();
        }
        System.out.println("Average Waiting Time: " + totalWaitingTime / finishedCloudlets.size());
    }

    /**
     * Creates a Datacenter and its Hosts.
     */
    private Datacenter createDatacenter() {
        final List<Host> hostList = new ArrayList<>(HOSTS);
        for(int h = 0; h < HOSTS; h++) {
            Host host = createHost();
            hostList.add(host);
        }

        final Datacenter dc = new DatacenterSimple(simulation, hostList, new VmAllocationPolicySimple());
        return dc;
    }

    private Host createHost() {
        List<Pe> peList = new ArrayList<>(HOST_PES);
        //List of Host's CPUs (Processing Elements, PEs)
        for (int i = 0; i < HOST_PES; i++) {
            peList.add(new PeSimple(VM_MIPS, new PeProvisionerSimple()));
        }

        final long ram = VM_RAM*100; //in Megabytes
        final long bw = VM_BW * 1000;; //in Megabits/s
        final long storage = VM_SIZE * 1000;; //in Megabytes
        ResourceProvisioner ramProvisioner = new ResourceProvisionerSimple();
        ResourceProvisioner bwProvisioner = new ResourceProvisionerSimple();
        VmScheduler vmScheduler = new VmSchedulerTimeShared();
        Host host = new HostSimple(ram, bw, storage, peList);
        host
            .setRamProvisioner(ramProvisioner)
            .setBwProvisioner(bwProvisioner)
            .setVmScheduler(vmScheduler);
        return host;
    }

    /**
     * Creates a list of VMs.
     */
    private List<Vm> createVms() {
        final List<Vm> list = new ArrayList<>(VMS);
        for (int v = 0; v < VMS; v++) {
            Vm vm =
                new VmSimple(v, VM_MIPS, VM_PES)
                    .setRam(VM_RAM).setBw(VM_BW).setSize(VM_SIZE)
                    .setCloudletScheduler(new CloudletSchedulerEDF());

            list.add(vm);
        }

        return list;
    }

    /**
     * Creates a list of Cloudlets.
     */
    private List<Cloudlet> createCloudlets() {
        final List<Cloudlet> list = new ArrayList<>(CLOUDLETS);
        int[] length = new int[]{6000, 15000, 66000, 7000};
        int[] arrival = new int[]{5, 6, 6, 6};
        double[] deadline = new double[]{12, 65, 281, 62};
        UtilizationModel utilization = new UtilizationModelFull();
        for (int c = 0; c < CLOUDLETS; c++) {
            Cloudlet cloudlet =
                new CloudletSimple(c, length[c], CLOUDLET_PES)
                    .setFileSize(1024)
                    .setOutputSize(1024)
                    .setUtilizationModel(utilization)
                ;
            cloudlet.setSubmissionDelay(arrival[c]);
            cloudlet.setDeadline(deadline[c]);
            list.add(cloudlet);
        }

        return list;
    }

    private List<Cloudlet> readCloudletsFromFile(String filename) {
        final List<Cloudlet> list = new ArrayList<>(CLOUDLETS);

        try(BufferedReader br = new BufferedReader(new FileReader(filename))) {
            //StringBuilder sb = new StringBuilder();
            String line = br.readLine();
            //Cloudlet properties
            int id = 0;
            double arrival;
            int pesNumber =1;
            long length;
            long memory;
            long storage;
            double deadline;
            UtilizationModel utilization = new UtilizationModelFull();
            while (line != null) {
                String[] data = line.split(" ");
                arrival = new Double(data[0].trim());
                length = new Long(data[1].trim());
                memory = new Long(data[2].trim());
                storage = new Long(data[3].trim());
                deadline = new Double(data[4].trim());

                Cloudlet cloudlet =
                    new CloudletSimple(id, length, CLOUDLET_PES)
                        .setFileSize(1024)
                        .setOutputSize(1024)
                        .setUtilizationModel(utilization)
                    ;
                cloudlet.setSubmissionDelay(arrival);
                cloudlet.setDeadline(deadline);
                list.add(cloudlet);

                line = br.readLine();
                id++;
            }
        }catch(IOException e){

        }
        return list;
    }

    private void createCloudletsFromWorkloadFile(DatacenterBroker broker) {
        SwfWorkloadFileReader reader = SwfWorkloadFileReader.getInstance(REAL_WORKLOAD_FILENAME, VM_MIPS);
        reader.setMaxLinesToRead(maximumNumberOfCloudletsToCreateFromTheWorkloadFile);
        this.cloudletList = reader.generateWorkload();

        System.out.printf("# Created %12d Cloudlets for %s%n", this.cloudletList.size(), broker);
    }
}
