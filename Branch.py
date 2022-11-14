import grpc
import bank_pb2
import bank_pb2_grpc
import time
import pdb
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
        # write_set
        self.write_set = []

    # TODO: students are expected to process requests from both Client and Branch
    def MsgDelivery(self, request, context):
        new_balance = 0
        op_result = bank_pb2.Result.failure
        operation_name = get_operation_name(request.operation_type)
        source_type = get_source_type_name(request.source_type)
        #if request is a query, sleep 3 seconds and make sure all of propagate completed
        if request.operation_type == bank_pb2.Operation.query:
            time.sleep(3)
            new_balance = self.balance
            op_result = bank_pb2.Result.success
        #if request from customer, run consistency check and propagate
        elif request.source_type == bank_pb2.Source.customer:
            #customer non first time request, need to verify the write_set
            if request.last_write_branch != 0 and request.last_write_id != 0:
                logger.info("Branch {} has received customer {} request, verifying the write_set....".format(self.id, request.id))
                logger.info("Customer request write_set {} : {}".format(request.last_write_branch, request.last_write_id))
                logger.info("Branch last write_set {} : {}".format(*self.write_set[-1]))
                if not self.is_write_set_consistency(request):
                    op_result = bank_pb2.Result.error
                    logger.info("Branch write_set verify failed....")
                    return self.Response(op_result, new_balance)
                logger.info("Branch write_set verify successful....")
            else:
                logger.info("Branch {} has received customer {} first time request, skip verify the write_set....".format(self.id, request.id))
                

            self.Event_Request(operation_name, source_type, request.id, request.clock)
            if request.operation_type == bank_pb2.Operation.withdraw:
                op_result, new_balance = self.WithDraw(request.amount)
            if request.operation_type == bank_pb2.Operation.deposit:
                op_result, new_balance = self.Deposit(request.amount)
            self.Event_Execute(operation_name, request.id)
            if op_result == bank_pb2.Result.success:
                self.write_id += 1
                self.write_set.append((self.id, self.write_id))
                self.Branch_Propagate(request.operation_type, request.amount)
                self.Event_Response(operation_name, request.id)

        #if request from branch, no progagate
        elif request.source_type == bank_pb2.Source.branch:
            #execute propagate request
            self.Propagate_Request(operation_name, source_type, request.id, request.clock)
            if request.operation_type == bank_pb2.Operation.withdraw:
                op_result, new_balance = self.WithDraw(request.amount)
            elif request.operation_type == bank_pb2.Operation.deposit:
                op_result, new_balance = self.Deposit(request.amount)
            #execute propagate execute
            self.Propagate_Execute(operation_name, request.id)
            #update local self.write_id from propagate request
            self.write_id = request.last_write_id
            #update the write_set from propagate request
            self.write_set.append((request.last_write_branch, request.last_write_id))
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
    
    def is_write_set_consistency(self, request):
        return self.write_set[-1] == (request.last_write_branch, request.last_write_id)

    def Deposit(self, amount):
        if amount < 0:
            return bank_pb2.Result.error, amount
        self.balance += amount
        return bank_pb2.Result.success, self.balance

    def WithDraw(self, amount):
        if amount > self.balance:
            return bank_pb2.Result.failure, amount
        self.balance -= amount
        return bank_pb2.Result.success, self.balance
    
    def Create_propagate_request(self, operation_type, amount):
        """
        Build the branch propagate request context
        """
        request = bank_pb2.MsgDelivery_request(
            operation_type = operation_type,
            source_type = bank_pb2.Source.branch,
            id = self.id,
            amount = amount,
            clock = self.clock,
            last_write_id = self.write_id,
            last_write_branch = self.id,
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

    def Branch_Propagate(self, operation_type, amount):
        """
        Run branches propagate
        If all of branches propagate return success, return list of branches propagate response clock
        """
        operation_name = get_operation_name(operation_type)
        if len(self.stubList) == 0:
            self.Create_branches_stub()
        for stub in self.stubList:
            propagate_request = self.Create_propagate_request(operation_type, amount)
            response = stub.MsgDelivery(propagate_request)
            logger.info("Propagate response {} from branch {}".format(
                get_result_name(response.operation_result), 
                response.id
                )
            )
            self.Propagate_Response(operation_name, response.clock)

    def Event_Request(self, operation_name, source_type, request_id, request_clock):
        self.clock = max(self.clock, request_clock) + 1
        operation_name = operation_name + "_request"
        self.events.append(
            {"name": operation_name, "clock": self.clock}
        )
        logger.info(
            "Branch {} has received {} from {} {}, clock is {}, local clock is changing to {}".format(
                self.id, 
                operation_name, 
                source_type, 
                request_id,
                request_clock,
                self.clock,
            )
        )
    
    def Event_Execute(self, operation_name, request_id):
        self.clock += 1
        operation_name = operation_name + "_execute"
        self.events.append(
            {"name": operation_name, "clock": self.clock}
        )
        logger.info("Branch {} {} , local clock changed to {}".format(self.id, operation_name,  self.clock))
    
    def Event_Response(self, operation_name, request_id):
        self.clock += 1
        operation_name = operation_name + "_response"
        self.events.append(
            {"name": operation_name, "clock": self.clock}
        )
        logger.info("Branch {} {} , local clock changed to {}".format(self.id, operation_name, self.clock))
    
    def Propagate_Request(self, operation_name, source_type, request_id, request_clock):
        self.clock = max(self.clock, request_clock) + 1
        operation_name = operation_name + "_propagate_request"
        self.events.append(
            {"name": operation_name, "clock": self.clock}
        )
        logger.info(
            "Branch {} has received {} from {} {}, clock is {}, local clock is changing to {}".format(
                self.id, 
                operation_name, 
                source_type, 
                request_id,
                request_clock,
                self.clock,
            )
        )

    def Propagate_Execute(self, operation_name, request_id):
        self.clock += 1
        operation_name = operation_name + "_propagate_execute"
        self.events.append(
            {"name": operation_name, "clock": self.clock}
        )
        logger.info("Branch {} {}, local clock changed to {}".format(self.id, operation_name, self.clock))
    
    def Propagate_Response(self, operation_name, response_clock):
        self.clock = max(response_clock, self.clock) + 1
        operation_name = operation_name + "_propagate_response"
        self.events.append(
            {"name": operation_name, "clock": self.clock}
        )
        logger.info("Branch {} {}, local clock changed to {}".format(self.id, operation_name, self.clock))
