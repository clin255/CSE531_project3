import grpc
import bank_pb2
import bank_pb2_grpc
import time
import pdb
import json
from utilities import configure_logger, get_operation_name, get_result_name, get_source_type_name
from concurrent import futures

logger = configure_logger("Branch")

class Branch(bank_pb2_grpc.BankServicer):

    def __init__(self, id, balance, branches, bind_addresses):
        # unique ID of the Branch
        self.id = id
        # replica of the Branch's balance
        self.balance = balance
        # the list of process IDs of the branches
        self.branches = branches
        # the list of Client stubs to communicate with the branches
        self.stubList = list()
        # a list of received messages used for debugging purpose
        self.events = list()
        # iterate the processID of the branches
        self.branches_bind_addresses = bind_addresses
        # local clock
        self.clock = 1
        # write_id
        self.write_id = 0
        # write_set based on customer id
        self.write_set = {}
        # write lock, make sure write operation completed before next write
        self.lock = False

    # TODO: students are expected to process requests from both Client and Branch
    def MsgDelivery(self, request, context):
        new_balance = 0
        op_result = bank_pb2.Result.failure
        operation_name = get_operation_name(request.operation_type)
        source_type = get_source_type_name(request.source_type)
        if request.operation_type == bank_pb2.Operation.query:
            #if customer request is first time request, create empty write_set for customer
            self.is_customer_first_operation(request)
            logger.info("Branch {} has received customer {} {} request, verifying the write_set....".format(self.id, request.id, operation_name))
            #Customer sent complete write_set, branch verify the write_set received if it is equal to local write_set
            if not self.check_write_set(request):
                op_result = bank_pb2.Result.error
                return self.Response(op_result, new_balance)
            new_balance = self.balance
            op_result = bank_pb2.Result.success
        #if request from customer, run propagate
        elif request.source_type == bank_pb2.Source.customer:
            logger.info("Branch {} has received customer {} {} request, verifying the write_set....".format(self.id, request.id, operation_name))
            #if customer request is first time request, create empty write_set for customer
            self.is_customer_first_operation(request)
            #Customer sent complete write_set, branch verify the write_set received is equal to local write_set
            if not self.check_write_set(request):
                op_result = bank_pb2.Result.error
                return self.Response(op_result, new_balance)
            if request.operation_type == bank_pb2.Operation.withdraw:
                op_result, new_balance = self.WithDraw(request.amount)
            if request.operation_type == bank_pb2.Operation.deposit:
                op_result, new_balance = self.Deposit(request.amount)
            if op_result == bank_pb2.Result.success:
                #if customer request is first time request, create empty write_set for customer
                if request.last_write_branch == 0 and request.last_write_id == 0:
                    self.write_set[request.id] = []
                self.write_id += 1
                self.write_set[request.id].append({"pid":self.id, "wid": self.write_id})
                self.Branch_Propagate(request.id, request.operation_type, request.amount)
            
        #if request from branch, no progagate
        elif request.source_type == bank_pb2.Source.branch:
            logger.info("Branch {} has received branch {} propagate request...".format(self.id, request.last_write_branch))
            if request.operation_type == bank_pb2.Operation.withdraw:
                op_result, new_balance = self.WithDraw(request.amount)
            elif request.operation_type == bank_pb2.Operation.deposit:
                op_result, new_balance = self.Deposit(request.amount)
            #update local self.write_id from propagate request
            self.write_id = request.last_write_id
            #if write_set does not have key for customer, that means customer first write, so create empty list for new customer
            self.is_customer_first_operation(request)
            #update the write_set from propagate request
            self.write_set[request.id].append({"pid":request.last_write_branch, "wid":request.last_write_id})
        #customer response or propagate response
        response = self.Response(op_result, new_balance)
        logger.info("Branch {} operation result {}, amount {}, clock {}, last_write_id {}".format(
            self.id, 
            get_result_name(op_result), 
            new_balance, 
            self.clock, 
            self.write_id
            )
        )
        #release the lock after completed the write operation
        self.lock = False
        #response customer or branch propagate
        return response
    
    def Response(self, op_result, new_balance):
        """
        Can be used for customer response or branch propagate response
        """
        response = bank_pb2.MsgDelivery_response(
            operation_result = op_result,
            source_type = bank_pb2.Source.branch,
            id = self.id,
            amount = new_balance,
            clock = self.clock,
            last_write_id = self.write_id
        )
        return response

    def is_customer_first_operation(self, request):
        """
        Check if customer is first operation, if it's create an empty write_set for customer.
        """
        if request.id not in self.write_set:
                self.write_set[request.id] = []

    def check_write_set(self, request):
        """
        Compare the write_set from customer and local write_set, if different, return False, otherwise return Ture.
        """
        if json.loads(request.write_set) != self.write_set[request.id]:
            logger.info("Branch write_set verify failed, customer write_set {} != branch write_set {}".format(
                request.write_set, 
                self.write_set[request.id]
                )
            )
            return False
        logger.info("Branch write_set verify succeed..customer write_set {} == local write_set {}".format(
            request.write_set,
            self.write_set[request.id]
            )
        )
        return True

    def Deposit(self, amount):
        if amount < 0:
            return bank_pb2.Result.error, amount
        #This is part of monotonic-write consistency implementation
        #if self.lock == True, which is indicated process doing write operation, have to wait
        while self.lock:
            time.sleep(1)
        self.balance += amount
        return bank_pb2.Result.success, self.balance

    def WithDraw(self, amount):
        if amount > self.balance:
            return bank_pb2.Result.failure, amount
        #This is part of monotonic-write consistency implementation
        #if self.lock == True, which is indicated process doing write operation, have to wait
        while self.lock:
            time.sleep(1)
        self.balance -= amount
        return bank_pb2.Result.success, self.balance
    
    def Create_propagate_request(self, customer_id, operation_type, amount):
        """
        Build the branch propagate request context
        """
        request = bank_pb2.MsgDelivery_request(
            operation_type = operation_type,
            source_type = bank_pb2.Source.branch,
            id = customer_id,
            amount = amount,
            clock = self.clock,
            last_write_id = self.write_id,
            last_write_branch = self.id,
            write_set = ""
        )
        return request

    def Create_branches_stub(self):
        """
        Create branches stub
        """
        for branch in self.branches:
            if branch != self.id:
                bind_address = self.branches_bind_addresses[branch]
                stub = bank_pb2_grpc.BankStub(grpc.insecure_channel(bind_address))
                self.stubList.append(stub)

    def Branch_Propagate(self, customer_id, operation_type, amount):
        """
        Run branches propagate
        If all of branches propagate return success, return list of branches propagate response clock
        """
        operation_name = get_operation_name(operation_type)
        if len(self.stubList) == 0:
            self.Create_branches_stub()
        logger.info("Branch {} start propagating ...".format(self.id))
        for stub in self.stubList:
            propagate_request = self.Create_propagate_request(customer_id, operation_type, amount)
            response = stub.MsgDelivery(propagate_request)
            logger.info("Propagate response {} from branch {}".format(
                get_result_name(response.operation_result), 
                response.id
                )
            )
