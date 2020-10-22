module DataSources

# Data Sources
# This file will create the data sources used for the pulse report
using Reexport
@reexport using DataFrames, VetschLib, Dates

function phs(max_date::Date; min_date::Date = Date(1900))
    out::DataFrame = sql(
        """
        set nocount on;
         drop table if exists #phs;

         select distinct ph.Contract_Id, ph.Finance_Product, ph.Invoice_Number, convert(date, ph.Due_Date) 'due_date', convert(date, ph.Invoice_Date) 'invoice_date',
         convert(date, ph.Date_Paid) 'date_paid', ph.check_number, ph.invoice_description, ph.Due_Amount,
         ph.Amount_Paid, ph.Principal_Amount, ph.interest_amount, ph.contract_is_charge_off, ph.entered_by,
         case
         	when Payment_Type in ('ACH', 'Check', 'N/A') and
         		Invoice_Description in('Loan Payment', 'Loan Payoff', 'Interest Payment') then 1
         	else 0
         end 'is_valid_invoice', case
         	when Check_Number like 'Credit Memo' then 1
         	else 0
         end 'is_credit_memo',  case
         	when aph.is_cm_invoice > 0 then 1
         	else 0
         end as is_credit_memo_invoice, case
         	when aph.is_cm_invoice = 1 then 0
         	else ph.Amount_Paid
         end 'actual_amount_paid', case
         	when Terminated = 0 then 1
         	else 0
         end 'is_active'
         into #phs
         from Report_Aspire_Payment_History ph
         left join(
         	select aph.Contract_Id, aph.Invoice_Number, sum(
         		case
         			when Check_Number like 'Credit Memo' then 1
         			else 0
         		end
         	) as 'is_cm_invoice'
         	from Report_Aspire_Payment_History aph
            where aph.due_date <= '$max_date' and
                aph.due_date >= '$min_date'
         	group by aph.Contract_Id, aph.Invoice_Number
         ) as aph on aph.Contract_Id = ph.Contract_Id and aph.Invoice_Number = ph.Invoice_Number
         where ph.due_date <= '$max_date';

         drop table if exists #phs2;

         select *
         into #phs2
         from #phs
         where is_valid_invoice = 1 and
         	is_credit_memo_invoice = 0;


         select Contract_Id, Invoice_Number, convert(datetime, min(due_date)) as due_date, Avg(Due_Amount) as due_amount, convert(datetime, max(date_paid)) as date_paid, sum(Amount_Paid) as amount_paid
         from #phs2
         group by Contract_Id, Invoice_Number
        """
    )

    transform!(
        out,
        :due_date => ByRow(
            x -> ismissing(x) ? missing : Date(x)
        ) => :due_date,
        :date_paid => ByRow(
            x -> ismissing(x) ? missing : Date(x)
        ) => :date_paid
    )

    return out

end # function

#=
    TODO: This definition is close, but not exact. I'll revisit it with
    Ed later.
=#


function allfundedcontracts(max_date::Date; min_date::Date = Date(1900))
    out::DataFrame = sql(
        """
        select lms.opportunity_id, lms.Contract_Id, convert(datetime, oc.Funding_Date) as funded_date,
        convert(datetime, lms.Termination_Date) as termination_date
        from Opportunity_Contract_LMS as lms
        inner join Opportunity_Contract as oc on oc.Aspire_Id = lms.Contract_Oid
        where oc.Funding_Date is not null and
        	lms.Contract_Id like '%-%' and
        	lms.Contract_Id not like '%-10%' and
        	lms.Contract_Id not like '%-1S' and
        	lms.Contract_Id not like '%TEST%' and
            oc.funding_date <= '$max_date' and
            oc.funding_date >= '$min_date'
        """
    )

    transform!(
        out,
        :funded_date => ByRow(
            x -> ismissing(x) ? missing : Date(x)
        ) => :funded_date,
        :termination_date => ByRow(
            x -> ismissing(x) ? missing : Date(x)
        ) => :termination_date
    )

    return out

end # function


# This function doesn't take an argument since the modification period has ended.
function covidmods()
    out::DataFrame = sql(
        """
        set nocount on;

        drop table if exists #events;

        select distinct contract_id, case
            when reason like 'Covid-19 Relief Addendum' and
                post_date >= '2020-03-24' then 1
            when reason like 'BA Reconciliation' and
                post_date >= '2020-03-24' and
                post_date < '2020-07-20' then 1
            else 0
        end as is_covid_modded
        into #events
        from Report_Aspire_Contract_Modification as cm;

        drop table if exists #mod_contracts;
        select distinct Contract_Id
        into #mod_contracts
        from #events
        where #events.is_covid_modded = 1;


        drop table if exists #mod_dates;

        select distinct mc.Contract_Id, convert(datetime, min(cm.post_date)) as mod_post_date
        into #mod_dates
        from #mod_contracts as mc
        left join Report_Aspire_Contract_Modification as cm on cm.Contract_Id = mc.Contract_Id
        group by mc.Contract_Id;

        select md.*, convert(datetime, sam.mod_effective_date) as mod_effective_date, convert(datetime, sam.subsequent_effective_date) as sub_effective_date
        from #mod_dates as md
        left join dbo.StaticAccountMod as sam on sam.contract_id = md.Contract_Id
        """
    )

    transform!(
        out,
        names(out, r"date") .=> ByRow(
            x -> ismissing(x) ? missing : Date(x)
        ) .=> names(out, r"date")
    )

    return out

end # function

function nonaccrualcontracts(max_date::Date; min_date::Date = Date(1900))
    out::DataFrame = sql(
        """
        select contract_id, balance_at_non_accrual, convert(datetime, non_accrual_date) as non_accrual_date
        from dbo.StaticNonAccrual
        where non_accrual_date <= '$max_date' and
            non_accrual_date >= '$min_date'
        """
    )

    # This is done in a map function because the number of columns needing to be
    # transformed is only one.
    out.non_accrual_date = map(out.non_accrual_date) do x
        Date(x)
    end

    return out
end # function

# This function takes no arguments because it is very fast and it's easier to
# do the filtering in the joins.
function industries()
    out::DataFrame = sql(
        """
        select opp.id as opportunity_id, lms.contract_id, coalesce(ict.Description, 'Unknown') as industry
        from Opportunity as opp
        inner join opportunity_contract as oc on oc.opportunity_id = opp.id
        inner join opportunity_contract_lms as lms on lms.contract_oid = oc.aspire_id
        inner join Business as bus on bus.Salesforce_Id = opp.Salesforce_Account_Id
        left join Industry_NAICS_Internal as ini on ini.NAICS = bus.NAICS
        left join Industry_Category_Type as ict on ict.Id = ini.Industry_Category_Id
        """
    )

    dropmissing!(out, :contract_id)

    return out
end

function trialbalance(max_date::Date; min_date::Date = Date(1900))
    tmp::DataFrame = sql(
    """
    select contract_id, balance, convert(datetime, posting_date) as post_date
    from report_aspire_trial_balance
    where contract_id is not null and
        posting_date <= '$max_date' and
        posting_date >= '$min_date'
    """
    )

    tmp.post_date = map(tmp.post_date) do x
        Date(x)
    end

    set_min_date::Date = min_date == Date(1900) ? minimum(tmp.post_date) : min_date

    out::DataFrame = DataFrame(
        :pull_date => Array{Date}(undef, 1),
        :contract_id => Array{String}(undef, 1),
        :balance => Array{Number}(undef, 1)
    )

    @inbounds for dt in set_min_date:Day(1):max_date
        h::DataFrame = filter(
            r -> r.post_date <= dt,
            tmp
        )
        h = combine(
            groupby(h, :contract_id),
            :post_date => maximum => :pull_date
        )
        h = innerjoin(
            h,
            tmp,
            on = [:contract_id => :contract_id, :pull_date => :post_date]
        )
        append!(out, h)
    end

    out = out[2:end, :] # removing the undef first row

    return out

end # function

end # module
