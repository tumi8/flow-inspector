#!/usr/bin/env python
# -*- coding: utf-8 -*-

class Node(object):
    
    """ The base class for all nodes """

    def __init__(self, ip, netmask):
        self.ip = ip
        self.netmask = netmask
        self.edges = set()
   
    def toGML(self):
        return '    <node id="{0}"></node>'.format(self.getID())   

    def getID(self):
       return str(self.ip) + "/" + str(self.netmask) 


class Router(Node):

    """ This class represents a router """

    def __init__(self, ip):
        super(self.__class__, self).__init__(ip, 32)

    def toGML(self):
        return '\n'.join([  '    <node id="{0}">',
                            '      <data key="r">255</data>',
                            '      <data key="label">{1}</data>',
                            '      <date key="z">10</date>',
                            '    </node>']).format(self.getID(), self.getLabel())

    def getID(self):
        return "router_" + str(self.ip) + "/32"

    def getLabel(self):
        return "Router " + str(self.ip)


class Interface(Node):

    """ This class represents a router interface """

    def __init__(self, ip, ifnumber, netmask):
        super(Interface, self).__init__(ip, 32)
        self.ifnumber = ifnumber
        self.netmask = netmask

    def toGML(self):
        return '\n'.join([  '    <node id="{0}">',
                            '      <data key="g">255</data>',
                            '      <data key="label">{1}</data>',
                            '    </node>']).format(self.getID(), self.getLabel())

    def getID(self):
        return str(self.ip) + "/32"
   
    def getLabel(self):
        return str(self.ifnumber) + "_" + str(self.ip) + "/" + str(self.netmask)
        
class Subnet(Node):
    
    """ This class represents a subnet """ 
    
    def __init__(self, ip, netmask):
        super(Subnet, self).__init__(ip, netmask)   

    def toGML(self):
        return '\n'.join([  '    <node id="{0}">',
                            '      <data key="b">255</data>',
                            '      <data key="label">{1}</data>',
                            '    </node>']).format(self.getID(), self.getLabel())

    def getID(self):
        return str(self.ip) + "/" + str(self.netmask)
    
    def getLabel(self):
        return str(self.ip) + "/" + str(self.netmask)


class Graph(object): 

    """ This class represents a graph containing nodes """

    def __init__(self):
        """ Instanciate new Graph object """
        self.graphdb = dict()
        self.interfacedb = dict()

#    def add_node(self, node):
#        """ Add Node object """
#        self.graphdb[node.ip + "/" + node.netmask] = node

#    def set_node_type(self, ip, netmask, new_type):
#        """ Set type of a node, create node if necessary """
#        key = str(ip) + "/" + str(netmask)        
#        if key not in self.graphdb:
#            self.graphdb[key] = Node(ip, netmask)
#        self.graphdb[key].__class__ = new_type

    def add_edge(self, ip_a, netmask_a, ip_b, netmask_b):
        """ Add edge between nodes, create nodes if necessary """
        key_a = str(ip_a) + "/" + str(netmask_a)
        key_b = str(ip_b) + "/" + str(netmask_b)
        if key_a not in self.graphdb:
            self.graphdb[key_a] = Node(ip_a, netmask_a)
        if key_b not in self.graphdb:
            self.graphdb[key_b] = Node(ip_b, netmask_b)
        self.graphdb[key_a].edges.add( self.graphdb[key_b] )
        if (netmask_a != 32): self.raphdb[key_a].__class__ = Subnet
        if (netmask_b != 32): self.graphdb[key_b].__class__ = Subnet

    def add_router(self, ip):
        """ Add router to graph """
        key = "router_" + ip + "/32"
        # create new Router node if necessary
        if key not in self.graphdb:
            self.graphdb[key] = Router(ip)
        # ensure that node type is router even if the node was created in advance
        self.graphdb[key].__class__ = Router
    
    def add_router_route(self, ip_router, if_number, ip_nexthop):
        """ Add route to next hop to router """
        key = str(ip_nexthop) + "/32"
        if key not in self.graphdb:
            self.graphdb[key] = Node(ip_nexthop, 32)
        if str(ip_router) + "_" + str(if_number) not in self.interfacedb:
            print ip_router + " / " + if_number
            self.add_router_interface(ip_router, "???", if_number)
        self.interfacedb[str(ip_router) + "_" + str(if_number)].edges.add(self.graphdb[key])
        
         
    def add_router_interface(self, ip_router, ip_interface, if_number, ip_netmask):
        """ Add interface to router """
        key = str(ip_interface) + "/32"
        if key not in self.graphdb:
            self.graphdb[key] = Interface(ip_interface, if_number, ip_netmask)
        self.graphdb[key].__class__ = Interface
        self.graphdb[key].ifnumber = if_number
        self.interfacedb[str(ip_router) + "_" + str(if_number)] = self.graphdb[key]
        if "router_" + ip_router + "/32" not in self.graphdb:
            self.add_router(ip_router)
        self.graphdb["router_" + ip_router + "/32"].edges.add( self.graphdb[key] )

    def add_router_indirect_route(self, ip_router, ip_dst, mask_dst, ip_nexthop, if_number):
        if str(ip_nexthop) + "/32" not in self.graphdb:
            if "router_" + ip_router + "/32" not in self.graphdb:
                self.add_router(ip_router)
            if if_number == "0":
                if str(ip_nexthop) + "/32" not in self.graphdb:
                    node = Node(ip_nexthop, 32)
                else:
                    node = self.graphdb[str(ip_nexthop) + "/32"]
                self.graphdb["router_" + ip_router + "/32"].edges.add(node)
            else:
                self.add_router_route(ip_router, if_number, ip_nexthop)
            self.add_edge(ip_nexthop, 32, ip_dst, mask_dst)




XMLHEADER = '\n'.join([
    '<?xml version="1.0" encoding="UTF-8"?>',
    '<graphml xmlns="http://graphml.graphdrawing.org/xmlns" '
    'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
    'xsi:schemaLocation="http://graphml.graphdrawing.org/xmlns '
    'http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd">',
    '  <graph id="g1" edgedefault="directed">',  
    '    <key attr.name="label" attr.type="string" for="node" id="label"/>',
    '    <key attr.name="Edge Label" attr.type="string" for="edge" id="edgelabel"/>',
    '    <key attr.name="weight" attr.type="double" for="edge" id="weight"/>',
    '    <key attr.name="Edge Id" attr.type="string" for="edge" id="edgeid"/>',
    '    <key attr.name="r" attr.type="int" for="node" id="r"/>',
    '    <key attr.name="g" attr.type="int" for="node" id="g"/>',
    '    <key attr.name="b" attr.type="int" for="node" id="b"/>',
    '    <key attr.name="x" attr.type="float" for="node" id="x"/>',
    '    <key attr.name="y" attr.type="float" for="node" id="y"/>',
    '    <key attr.name="z" attr.type="float" for="node" id="z"/>'
    '    <key attr.name="size" attr.type="float" for="node" id="size"/>'
])

XMLFOOTER = '\n'.join([
    '  </graph>',
    '</graphml>',
])

EDGE_TEMPLATE = '<edge id="e{0}" source="{1}" target="{2}"/>'

def graph_to_graphmlfile(graph, outputfile):
    """Export the graph into a graphml file"""
    # utf-8 encoding is default to my knowledge (see pep-3120)
    handle = open(outputfile, 'w')
    handle.write(XMLHEADER)
    handle.write('\n')
    indent = 4 * ' '

    # write nodes first (otherwise gephi complains)
    for node in graph.graphdb.itervalues():
        #handle.write(indent)
        handle.write(node.toGML())
        handle.write('\n')

    # write edges
    edge_id = 0
    for node in graph.graphdb.itervalues():
        for dst in node.edges:
            handle.write(indent)
            handle.write(EDGE_TEMPLATE.format(edge_id, node.getID(), dst.getID()))
            handle.write('\n')
            edge_id += 1
    handle.write(XMLFOOTER)
    handle.close()
