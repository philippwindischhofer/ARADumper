import ROOT, numpy, sys, os

ROOT.gSystem.Load("libnuphaseroot.so")

class Reader:

  def __init__(self, run, base_dir=None, header_type="filtered"):
    
    if base_dir == None: 
        base_dir = os.environ["NUPHASE_ROOT_DATA"] 
    self.run = int(run); 
    self.base_dir = base_dir

    self.event_file = ROOT.TFile.Open("%s/run%d/event.root" % (base_dir, run))
    self.event_tree = self.event_file.Get("event") 

    event_N = self.event_tree.Draw("Entry$:10*int(event.event_number % 1000000000) + event.board_id[0]","","goff") # this helps with surface triggers, I think

    unsorted_event_entries = numpy.frombuffer(self.event_tree.GetV1(), numpy.dtype('float64'), event_N) 
    unsorted_hashed_event_numbers = numpy.frombuffer(self.event_tree.GetV2(), numpy.dtype('float64'), event_N) 

    # make sure the index is sorted
    sorted_indices = numpy.argsort(unsorted_hashed_event_numbers)
    self.event_entries = unsorted_event_entries[sorted_indices]
    self.hashed_event_numbers = unsorted_hashed_event_numbers[sorted_indices]

    self.event_tree.BuildIndex("event.event_number")
    self.event_entry = -1; 

    header_file_name = "header.root" 
    if header_type is not None: 
        header_file_name = "header.%s.root" % (header_type)

    self.head_file = ROOT.TFile.Open("%s/run%d/%s" % (base_dir, run,header_file_name))
    self.head_tree = self.head_file.Get("header") 
    self.head_entry = -1
    self.head_tree.BuildIndex("header.event_number % 1000000000", "header.trigger_type==4") 

    header_N = self.head_tree.Draw("Entry$", "header.trigger_type!=4","goff") 
    self.deep_entries = numpy.copy(numpy.frombuffer(self.head_tree.GetV1(), numpy.dtype('float64'), header_N))

    self.status_file = ROOT.TFile.Open("%s/run%d/status.root" % (base_dir, run))
    self.status_tree = self.status_file.Get("status") 
    self.status_tree.BuildIndex("status.readout_time","status.readout_time_ns") 
    self.status_entry =-1; 

    self.current_entry = 0; 

  def numberEntries(self):
    return self.head_tree.GetEntries()
    
  def setEntry(self,i): 
    if (i < 0 or i >= self.head_tree.GetEntries()):
      sys.stderr.write("Entry out of bounds!\n") 
    else: 
      self.current_entry = i; 

  def setEvent(self,i, surface=0):
    self.setEntry(self.head_tree.GetEntryNumberWithIndex(i % 1000000000, 1 if surface else 0)) 

  def setDeepEntry(self,i):
    self.setEntry(int(self.deep_entries[i]))

  def event(self,force_reload = False): 
    if (self.event_entry != self.current_entry or force_reload):
      hashed_ev_number = 10*(self.header(force_reload).getEventNumber() % 1000000000 )
      hashed_ev_number+=self.header().getBoardID(); 
      i_entry = numpy.searchsorted(self.hashed_event_numbers,hashed_ev_number) 
      i = int(self.event_entries[i_entry])
      self.event_tree.GetEntry(i) 
      self.event_entry = self.current_entry 
      assert(self.event_tree.event.getEventNumber() == self.header().getEventNumber()) , str(self.event_tree.event.getEventNumber()) + " (event) does not equal " + str(self.header().getEventNumber()) + " (header)"
    return self.event_tree.event


  def wf(self,ch = 0, force = False):  

    ev = self.event() 
    bd = ROOT.nuphase.BOARD_MASTER;
    if (ch > 8) :
        ch-=8
        bd = ROOT.nuphase.BOARD_SLAVE
    ## stupid hack because for some reason it doesn't always report the right buffer length 
    return numpy.frombuffer(ev.getData(ch,bd), numpy.dtype('float64'), ev.getBufferLength()) - 64 

  def t(self):
    return numpy.linspace(0, self.event().getBufferLength() /1.5, self.event().getBufferLength()) 

  def header(self,force_reload = False): 
    if (self.head_entry != self.current_entry or force_reload): 
      self.head_tree.GetEntry(self.current_entry); 
      self.head_entry = self.current_entry 
    return self.head_tree.header

  def status(self,force_reload = False): 
    if (self.status_entry != self.current_entry or force_reload): 
      self.status_tree.GetEntry(self.status_tree.GetEntryNumberWithBestIndex(self.header().getReadoutTime(), self.header().getReadoutTimeNs()))
      self.status_entry = self.current_entry

    return self.status_tree.status


  def N(self): 
    return self.head_tree.GetEntries() 
