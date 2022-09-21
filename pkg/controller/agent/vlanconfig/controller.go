package vlanconfig

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"

	ctlcorev1 "github.com/rancher/wrangler/pkg/generated/controllers/core/v1"
	"github.com/vishvananda/netlink"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/klog/v2"

	"github.com/harvester/harvester-network-controller/pkg/apis/network.harvesterhci.io"
	networkv1 "github.com/harvester/harvester-network-controller/pkg/apis/network.harvesterhci.io/v1beta1"
	"github.com/harvester/harvester-network-controller/pkg/config"
	"github.com/harvester/harvester-network-controller/pkg/controller/agent/nad"
	ctlnetworkv1 "github.com/harvester/harvester-network-controller/pkg/generated/controllers/network.harvesterhci.io/v1beta1"
	"github.com/harvester/harvester-network-controller/pkg/network/iface"
	"github.com/harvester/harvester-network-controller/pkg/network/vlan"
	"github.com/harvester/harvester-network-controller/pkg/utils"
	ctlcniv1 "github.com/harvester/harvester/pkg/generated/controllers/k8s.cni.cncf.io/v1"
)

const (
	ControllerName = "harvester-network-vlanconfig-controller"
	bridgeCNIName  = "bridge"
)

type Handler struct {
	nodeName   string
	nodeClient ctlcorev1.NodeClient
	nodeCache  ctlcorev1.NodeCache
	nadCache   ctlcniv1.NetworkAttachmentDefinitionCache
	vcCache    ctlnetworkv1.VlanConfigCache
	vsClient   ctlnetworkv1.VlanStatusClient
	vsCache    ctlnetworkv1.VlanStatusCache
	cnClient   ctlnetworkv1.ClusterNetworkClient
	cnCache    ctlnetworkv1.ClusterNetworkCache
}

func Register(ctx context.Context, management *config.Management) error {
	vcs := management.HarvesterNetworkFactory.Network().V1beta1().VlanConfig()
	vss := management.HarvesterNetworkFactory.Network().V1beta1().VlanStatus()
	cns := management.HarvesterNetworkFactory.Network().V1beta1().ClusterNetwork()
	nodes := management.CoreFactory.Core().V1().Node()
	nads := management.CniFactory.K8s().V1().NetworkAttachmentDefinition()

	handler := &Handler{
		nodeName:   management.Options.NodeName,
		nodeClient: nodes,
		nodeCache:  nodes.Cache(),
		nadCache:   nads.Cache(),
		vcCache:    vcs.Cache(),
		vsClient:   vss,
		vsCache:    vss.Cache(),
		cnClient:   cns,
		cnCache:    cns.Cache(),
	}

	vcs.OnChange(ctx, ControllerName, handler.OnChange)
	vcs.OnRemove(ctx, ControllerName, handler.OnRemove)

	return nil
}

func (h Handler) OnChange(key string, vc *networkv1.VlanConfig) (*networkv1.VlanConfig, error) {
	if vc == nil || vc.DeletionTimestamp != nil {
		return nil, nil
	}
	klog.Infof("vlan config %s has been changed, spec: %+v", vc.Name, vc.Spec)

	ok, v, err := h.MatchNode(vc)
	if err != nil {
		return nil, err
	}
	// Not match
	if !ok {
		if err := h.removeVLAN(vc); err != nil {
			return nil, err
		}
		return vc, nil
	}
	// Another vlanConfig has take effect on this node
	if v != "" && v != vc.Name {
		klog.Infof("vlanConfig %s matches the node %s but goes after %s", vc.Name, h.nodeName, v)
		return vc, nil
	}
	// set up VLAN
	if err := h.setupVLAN(vc); err != nil {
		return nil, err
	}

	return vc, nil
}

func (h Handler) OnRemove(key string, vc *networkv1.VlanConfig) (*networkv1.VlanConfig, error) {
	klog.Infof("vlan config %s has been removed", vc.Name)

	if err := h.removeVLAN(vc); err != nil {
		return nil, err
	}
	return nil, nil
}

func (h Handler) MatchNode(vc *networkv1.VlanConfig) (bool, string, error) {
	if vc.Annotations == nil || vc.Annotations[utils.KeyMatchedNodes] == "" {
		return false, "", nil
	}

	var matchedNodes []string
	if err := json.Unmarshal([]byte(vc.Annotations[utils.KeyMatchedNodes]), &matchedNodes); err != nil {
		return false, "", nil
	}

	for _, n := range matchedNodes {
		if n == h.nodeName {
			node, err := h.nodeCache.Get(n)
			if err != nil {
				return false, "", err
			}
			return true, node.Labels[utils.KeyVlanConfigLabel], nil
		}
	}

	return false, "", nil
}

func (h Handler) setupVLAN(vc *networkv1.VlanConfig) error {
	var v *vlan.Vlan
	var setupErr error
	var localAreas []*vlan.LocalArea
	var uplink *iface.Link
	// get VIDs
	localAreas, setupErr = h.getLocalAreas(iface.GenerateName(vc.Spec.ClusterNetwork, iface.BridgeSuffix))
	if setupErr != nil {
		goto updateStatus
	}
	// construct uplink
	uplink, setupErr = setUplink(vc)
	if setupErr != nil {
		goto updateStatus
	}
	// set up VLAN
	v = vlan.NewVlan(vc.Spec.ClusterNetwork, localAreas)
	setupErr = v.Setup(uplink)

updateStatus:
	// Update status and still return setup error if not nil
	if err := h.updateStatus(vc, v, setupErr); err != nil {
		return fmt.Errorf("update status into vlanstatus %s failed, error: %w, setup error: %v",
			h.statusName(vc.Name), err, setupErr)
	}
	if setupErr != nil {
		return fmt.Errorf("set up VLAN failed, vlanconfig: %s, node: %s, error: %w", vc.Name, h.nodeName, setupErr)
	}
	// update node labels for pod scheduling
	if err := h.addNodeLabel(vc); err != nil {
		return fmt.Errorf("add node label to node %s for vlanconfig %s failed, error: %w", h.nodeName, vc.Name, err)
	}

	return nil
}

func (h Handler) removeVLAN(vc *networkv1.VlanConfig) error {
	var v *vlan.Vlan
	var teardownErr error

	v, teardownErr = vlan.GetVlan(vc.Spec.ClusterNetwork)
	// We take it granted that `LinkNotFound` means the VLAN has been torn down.
	if teardownErr != nil {
		if errors.As(teardownErr, &netlink.LinkNotFoundError{}) {
			teardownErr = nil
		}
		goto updateStatus
	}
	if teardownErr = v.Teardown(); teardownErr != nil {
		goto updateStatus
	}

updateStatus:
	if err := h.removeNodeLabel(vc); err != nil {
		return err
	}
	if err := h.deleteStatus(vc, teardownErr); err != nil {
		return fmt.Errorf("update status into vlanstatus %s failed, error: %w, teardown error: %v",
			h.statusName(vc.Name), err, teardownErr)
	}
	if teardownErr != nil {
		return fmt.Errorf("tear down VLAN failed, vlanconfig: %s, node: %s, error: %w", vc.Name, h.nodeName, teardownErr)
	}

	return nil
}

func setUplink(vc *networkv1.VlanConfig) (*iface.Link, error) {
	// set link attributes
	linkAttrs := netlink.NewLinkAttrs()
	linkAttrs.Name = vc.Spec.ClusterNetwork + iface.BondSuffix
	if vc.Spec.Uplink.LinkAttrs != nil {
		if vc.Spec.Uplink.LinkAttrs.MTU != 0 {
			linkAttrs.MTU = vc.Spec.Uplink.LinkAttrs.MTU
		}
		if vc.Spec.Uplink.LinkAttrs.TxQLen != 0 {
			linkAttrs.TxQLen = vc.Spec.Uplink.LinkAttrs.TxQLen
		}
		if vc.Spec.Uplink.LinkAttrs.HardwareAddr != nil {
			linkAttrs.HardwareAddr = vc.Spec.Uplink.LinkAttrs.HardwareAddr
		}
	}
	// Note: do not use &netlink.Bond{}
	bond := netlink.NewLinkBond(linkAttrs)
	// set bonding mode
	mode := netlink.BOND_MODE_ACTIVE_BACKUP
	if vc.Spec.Uplink.BondOptions != nil && vc.Spec.Uplink.BondOptions.Mode != "" {
		mode = netlink.StringToBondMode(string(vc.Spec.Uplink.BondOptions.Mode))
	}
	bond.Mode = mode
	// set bonding miimon
	if vc.Spec.Uplink.BondOptions != nil && vc.Spec.Uplink.BondOptions.Miimon != 0 {
		bond.Miimon = vc.Spec.Uplink.BondOptions.Miimon
	}

	b := iface.NewBond(bond, vc.Spec.Uplink.NICs)
	if err := b.EnsureBond(); err != nil {
		return nil, err
	}

	return &iface.Link{Link: b}, nil
}

func (h Handler) getLocalAreas(bridgeName string) ([]*vlan.LocalArea, error) {
	nads, err := h.nadCache.List("", labels.Everything())
	if err != nil {
		return nil, fmt.Errorf("list nad failed, error: %v", err)
	}

	localAreas := make([]*vlan.LocalArea, 0)
	for _, n := range nads {
		netconf := &utils.NetConf{}
		if err := json.Unmarshal([]byte(n.Spec.Config), netconf); err != nil {
			return nil, fmt.Errorf("unmarshal failed, error: %w, value: %s", err, n.Spec.Config)
		}

		if netconf.Type == bridgeCNIName && netconf.BrName == bridgeName {
			klog.Infof("add nad:%s with vid:%d to the list", n.Name, netconf.Vlan)
			localArea, err := nad.GetLocalArea(n)
			if err != nil {
				return nil, fmt.Errorf("failed to get local area from nad %s/%s, error: %w", n.Namespace, n.Name, err)
			}
			localAreas = append(localAreas, localArea)
		}
	}

	return localAreas, nil
}

func (h Handler) updateStatus(vc *networkv1.VlanConfig, v *vlan.Vlan, setupErr error) error {
	var vStatus *networkv1.VlanStatus
	name := h.statusName(vc.Name)
	vs, getErr := h.vsCache.Get(name)
	if getErr != nil && !apierrors.IsNotFound(getErr) {
		return fmt.Errorf("could not get vlanstatus %s, error: %w", name, getErr)
	} else if apierrors.IsNotFound(getErr) {
		vStatus = &networkv1.VlanStatus{
			ObjectMeta: metav1.ObjectMeta{
				Name: name,
				Labels: map[string]string{
					utils.KeyVlanConfigLabel:     vc.Name,
					utils.KeyClusterNetworkLabel: vc.Spec.ClusterNetwork,
					utils.KeyNodeLabel:           h.nodeName,
				},
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion: vc.APIVersion,
						Kind:       vc.Kind,
						Name:       vc.Name,
						UID:        vc.UID,
					},
				},
			},
		}
	} else {
		vStatus = vs.DeepCopy()
	}

	vStatus.Labels = map[string]string{
		utils.KeyClusterNetworkLabel: vc.Spec.ClusterNetwork,
		utils.KeyVlanConfigLabel:     vc.Name,
		utils.KeyNodeLabel:           h.nodeName,
	}
	vStatus.Status.ClusterNetwork = vc.Spec.ClusterNetwork
	vStatus.Status.VlanConfig = vc.Name
	vStatus.Status.Node = h.nodeName
	if setupErr == nil {
		networkv1.Ready.SetStatusBool(vStatus, true)
		networkv1.Ready.Message(vStatus, "")
		vStatus.Status.LocalAreas = []networkv1.LocalArea{}
		for _, la := range v.ListLocalArea() {
			vStatus.Status.LocalAreas = append(vStatus.Status.LocalAreas, networkv1.LocalArea{
				VID:  la.Vid,
				CIDR: la.Cidr,
			})
		}
		vStatus.Status.LinkStatus = []networkv1.LinkStatus{
			{
				Name:        v.Bridge().Name,
				Index:       v.Bridge().Index,
				Type:        v.Bridge().Type(),
				MAC:         v.Bridge().HardwareAddr.String(),
				Promiscuous: v.Bridge().Promisc != 0,
				State:       v.Bridge().Attrs().OperState.String(),
				MasterIndex: v.Bridge().MasterIndex,
			},
			{
				Name:        v.Uplink().Attrs().Name,
				Index:       v.Uplink().Attrs().Index,
				Type:        v.Uplink().Type(),
				MAC:         v.Uplink().Attrs().HardwareAddr.String(),
				Promiscuous: v.Uplink().Attrs().Promisc != 0,
				State:       v.Uplink().Attrs().OperState.String(),
				MasterIndex: v.Uplink().Attrs().MasterIndex,
			},
		}
	} else {
		networkv1.Ready.SetStatusBool(vStatus, false)
		networkv1.Ready.Message(vStatus, setupErr.Error())
	}

	if getErr != nil {
		if _, err := h.vsClient.Create(vStatus); err != nil {
			return fmt.Errorf("failed to create vlanstatus %s, error: %w", name, err)
		}
	} else {
		if _, err := h.vsClient.Update(vStatus); err != nil {
			return fmt.Errorf("failed to update vlanstatus %s, error: %w", name, err)
		}
	}

	return nil
}

func (h Handler) deleteStatus(vc *networkv1.VlanConfig, teardownErr error) error {
	name := h.statusName(vc.Name)
	vs, err := h.vsCache.Get(name)
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("could not get vlanstatus %s, error: %w", name, err)
	} else if apierrors.IsNotFound(err) {
		return nil
	}

	if teardownErr != nil {
		vsCopy := vs.DeepCopy()
		networkv1.Ready.SetStatusBool(vsCopy, false)
		networkv1.Ready.Message(vsCopy, teardownErr.Error())
		if _, err := h.vsClient.Update(vsCopy); err != nil {
			return fmt.Errorf("failed to update vlanstatus %s, error: %w", name, err)
		}
	} else {
		if err := h.vsClient.Delete(name, &metav1.DeleteOptions{}); err != nil {
			return fmt.Errorf("failed to delete vlanstatus %s, error: %w", name, err)
		}
	}

	return nil
}

// vlanstatus name: <vc name>-<node name>-<crc32 checksum>
func (h Handler) statusName(vcName string) string {
	name := vcName + "-" + h.nodeName
	digest := crc32.ChecksumIEEE([]byte(name))
	suffix := fmt.Sprintf("%08x", digest)
	// The name contains no more than 63 characters
	maxLengthOfName := 63 - 1 - len(suffix)
	if len(name) > maxLengthOfName {
		name = name[:maxLengthOfName]
	}

	return name + "-" + suffix
}

func (h Handler) addNodeLabel(vc *networkv1.VlanConfig) error {
	node, err := h.nodeCache.Get(h.nodeName)
	if err != nil {
		return err
	}
	// Since the length of cluster network isn't bigger than 12, the length of key will less than 63.
	key := network.GroupName + "/" + vc.Spec.ClusterNetwork
	if node.Labels != nil && node.Labels[key] == utils.ValueTrue &&
		node.Labels[utils.KeyVlanConfigLabel] == vc.Name {
		return nil
	}

	nodeCopy := node.DeepCopy()
	if nodeCopy.Labels == nil {
		nodeCopy.Labels = make(map[string]string)
	}
	nodeCopy.Labels[key] = utils.ValueTrue
	nodeCopy.Labels[utils.KeyVlanConfigLabel] = vc.Name

	if _, err := h.nodeClient.Update(nodeCopy); err != nil {
		return fmt.Errorf("add labels for vlanconfig %s to node %s failed, error: %w", vc.Name, h.nodeName, err)
	}

	return nil
}

func (h Handler) removeNodeLabel(vc *networkv1.VlanConfig) error {
	node, err := h.nodeCache.Get(h.nodeName)
	if err != nil {
		return err
	}

	key := network.GroupName + "/" + vc.Spec.ClusterNetwork
	if node.Labels != nil && (node.Labels[key] == utils.ValueTrue ||
		node.Labels[utils.KeyVlanConfigLabel] == vc.Name) {
		nodeCopy := node.DeepCopy()
		delete(nodeCopy.Labels, key)
		delete(nodeCopy.Labels, utils.KeyVlanConfigLabel)
		if _, err := h.nodeClient.Update(nodeCopy); err != nil {
			return fmt.Errorf("remove labels for vlanconfig %s from node %s failed, error: %w", vc.Name, h.nodeName, err)
		}
	}

	return nil
}
