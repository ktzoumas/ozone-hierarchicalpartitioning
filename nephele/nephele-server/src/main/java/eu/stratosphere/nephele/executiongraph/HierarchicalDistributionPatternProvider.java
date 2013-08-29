package eu.stratosphere.nephele.executiongraph;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.io.DistributionPattern;

public class HierarchicalDistributionPatternProvider {
	
	private static final Log LOG = LogFactory.getLog(HierarchicalDistributionPatternProvider.class);
	
	private int numberOfSourceTasks;
	
	private int numberOfTargetTasks;
	
	private int numberOfSourceTeams;
	
	private int numberOfTargetTeams;
	
	private ArrayList<Node> sourceNodes;
	private ArrayList<Team> sourceTeams;
	
	private ArrayList<Node> targetNodes;
	private ArrayList<Team> targetTeams;
	
	private HashMap<Node,ArrayList<Node>> connectedTo;
	
	private class KeyRange {
		public int min;
		public int max;
		
		public KeyRange(int min, int max) 
		{
			this.min = min;
			this.max = max;
		}
		
		public boolean overlaps (KeyRange k) {
			if (k.min >= this.max)
				return false;
			else if (k.max <= this.min)
				return false;
			return true;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + getOuterType().hashCode();
			result = prime * result + max;
			result = prime * result + min;
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			KeyRange other = (KeyRange) obj;
			if (!getOuterType().equals(other.getOuterType()))
				return false;
			if (max != other.max)
				return false;
			if (min != other.min)
				return false;
			return true;
		}

		private HierarchicalDistributionPatternProvider getOuterType() {
			return HierarchicalDistributionPatternProvider.this;
		}

		@Override
		public String toString() {
			return "KeyRange [min=" + min + ", max=" + max + "]";
		}
		
		
		
	}
	
	private class Node {
		public int indexInStage;
		public int indexInTeam;
		public Team team;
		public ArrayList<Node> connectedTo;
		
		public Node(int indexInStage, int indexInTeam, Team team) 
		{
			this.indexInStage = indexInStage;
			this.indexInTeam = indexInTeam;
			this.team = team;
			connectedTo = new ArrayList<Node> ();
		}
		
		public void connectNode (Node n)
		{
			connectedTo.add(n);
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + getOuterType().hashCode();
			result = prime * result + indexInStage;
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			Node other = (Node) obj;
			if (!getOuterType().equals(other.getOuterType()))
				return false;
			if (indexInStage != other.indexInStage)
				return false;
			return true;
		}

		private HierarchicalDistributionPatternProvider getOuterType() {
			return HierarchicalDistributionPatternProvider.this;
		}

		@Override
		public String toString() {
			String targets = "";
			for (Node t: connectedTo)
				targets += t.indexInStage + ", ";
			return "Node [indexInStage=" + indexInStage + ", indexInTeam="
					+ indexInTeam + ", team=" + team.indexInStage + 
					", connected targets=" + targets + "]\n";
		}
		
		
		
	}
	
	private class Team {
		public ArrayList<Node> nodes;
		public int indexInStage;
		public KeyRange keyRange;
		public int size;
		
		public Team(int indexInStage, KeyRange keyRange, int size) 
		{
			this.indexInStage = indexInStage;
			this.keyRange = keyRange;
			this.size = size;
			nodes = new ArrayList<Node> ();
		}

		@Override
		public String toString() {
			return "Team [nodes=" + nodes + ", indexInStage=" + indexInStage
					+ ", keyRange=" + keyRange + ", size=" + size + "]\n";
		}
		
		
		
	}

	public HierarchicalDistributionPatternProvider(int numberOfSourceTasks,
			int numberOfTargetTasks, int numberOfSourceTeams,
			int numberOfTargetTeams) 
	{
		this.numberOfSourceTasks = numberOfSourceTasks;
		this.numberOfTargetTasks = numberOfTargetTasks;
		this.numberOfSourceTeams = numberOfSourceTeams;
		this.numberOfTargetTeams = numberOfTargetTeams;
	
		//sourceNodes = new ArrayList<Node> ();
		//targetNodes = new ArrayList<Node> ();
		//sourceTeams = new ArrayList<Team> ();
		//targetTeams = new ArrayList<Team> ();
		
		
		sourceNodes = createStage (numberOfSourceTasks, numberOfSourceTeams);
		targetNodes = createStage (numberOfTargetTasks, numberOfTargetTeams);
		sourceTeams = createTeamsFromStage (sourceNodes);
		targetTeams = createTeamsFromStage (targetNodes);		
		
		LOG.info ("Called with " + numberOfSourceTasks + " source tasks, " + 
				numberOfTargetTasks + " target tasks, " + numberOfSourceTeams + 
				" source teams, " + numberOfTargetTeams + " target teams.");
		LOG.info ("Source nodes:\n");
		LOG.info(sourceNodes.toString());
		LOG.info ("Target nodes:\n");
		LOG.info(targetNodes.toString());
		LOG.info ("Source teams:\n");
		LOG.info(sourceTeams.toString());
		LOG.info ("Target teams:\n");
		LOG.info(targetTeams.toString());
		
		connectStages ();
		
		LOG.info ("Source nodes connected:\n");
		LOG.info(sourceNodes.toString());
	}
	
	
	private ArrayList<Node> createStage (int numberOfTasks, int numberOfTeams)
	{
		ArrayList<Node> nodes = new ArrayList<Node> ();
		//ArrayList<Node> nodesInTeam = new ArrayList<Node> ();
		
		Team t = null;
		KeyRange kr = null;
		int firstNodeInTeam = 0;
		
		int sizeOfTeam = numberOfTasks / numberOfTeams;

		for (int i = 0; i < numberOfTasks; i++) {
			
			if (i % sizeOfTeam == 0) {
				kr = new KeyRange (i, i + sizeOfTeam);
				t = new Team (i / sizeOfTeam, kr, sizeOfTeam);
				firstNodeInTeam = i;
				//t.nodes.addAll (nodesInTeam);
				//nodesInTeam.clear();
				
			}
			Node n = new Node (i, i - firstNodeInTeam, t);
			nodes.add(n);
			//nodesInTeam.add (n);
			t.nodes.add(n);		
		}
		if (nodes.size() != numberOfSourceTasks) {
			System.exit(-1);
		}
		return nodes;

	}
	
	private ArrayList<Team> createTeamsFromStage (ArrayList<Node> stage)
	{
		ArrayList<Team> teams = new ArrayList<Team> ();
		teams.add (stage.get(0).team);
		Node prev = stage.get(0);
		Node n = null;
		for (int i = 1; i < stage.size(); i++) {
			n = stage.get(i);
			if (!n.team.equals(prev.team))
				teams.add(n.team);
			prev = n;
		}
		return teams;
	}
	
	
	private void connectStages ()
	{
		for (Team sourceTeam: sourceTeams) 
			for (Team targetTeam: targetTeams) 
				if (sourceTeam.keyRange.overlaps(targetTeam.keyRange))
					connectTeams (sourceTeam, targetTeam);
	}
	
	private void connectTeams (Team sourceTeam, Team targetTeam) 
	{
		// Round-robin
		int i = 0;
		Node t = targetTeam.nodes.get(i);
		for (Node s: sourceTeam.nodes) {
			s.connectNode (t);
			i = (i + 1) % targetTeam.size;
			t = targetTeam.nodes.get(i);
		}
	}
	
	
	public boolean createWire (final int nodeLowerStage, final int nodeUpperStage)
	{
		Node s = sourceNodes.get (nodeLowerStage);
		Node t = targetNodes.get (nodeUpperStage);
		for (int i = 0; i < s.connectedTo.size(); i++) {
			Node n = s.connectedTo.get(i);
			if (n.indexInStage == t.indexInStage)
				return true;
		}
		return false;
		/*
		if (s.connectedTo.contains(t)) 
			return true;
		else
			return false;
			*/
	}

}
