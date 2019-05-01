from django import forms


class DateInput(forms.DateInput):
    # Creating a custom widget because default DateInput doesn't use
    # input type="date"
    input_type = 'date'


class FilterForm(forms.Form):

    start_date = forms.DateField(required=False, label="Start date:",
                                 widget=DateInput())
    end_date = forms.DateField(required=False, label="End date:",
                               widget=DateInput())

    limit_to_user_list = forms.BooleanField(required=False)
